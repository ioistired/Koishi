import asyncio
import datetime
import logging
import os.path
from io import BytesIO

import aiohttp
import discord
from discord.ext import commands
from PIL import Image
from yarl import URL

from .utils import images

logger = logging.getLogger(__name__)

scheme = {
         'names' : {
            'uid' : 'BIGINT',
            'name' : 'TEXT',
            'first_seen' : 'TIMESTAMP WITHOUT TIME ZONE'
            },
         'discrims' : {
            'uid' : 'BIGINT',
            'discrim' : 'TEXT',
            'first_seen' : 'TIMESTAMP WITHOUT TIME ZONE'
            },
         'nicks' : {
            'uid' : 'BIGINT',
            'sid' : 'BIGINT',
            'nick' : 'TEXT',
            'first_seen' : 'TIMESTAMP WITHOUT TIME ZONE'
            },
         'statuses' : {
            'uid' : 'BIGINT',
            'status' : 'TEXT',
            'first_seen' : 'TIMESTAMP WITHOUT TIME ZONE'
            },
         }
scheme2 = {
         'names' : {
            'key' : 'uid',
            'value' : 'name',
            },
         'discrims' : {
            'key' : 'uid',
            'value' : 'discrim',
            },
         'nicks' : {
            'key' : 'uid, sid',
            'value' : 'nick',
            },
         'statuses' : {
            'key' : 'uid',
            'value' : 'status',
            },
         }


class Pop(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.logger = logging.getLogger('koishi')
        self.bg_tasks = {recordtype : self.bot.loop.create_task(self.batching_task(recordtype)) for recordtype in scheme.keys()}
        self.sync_task = self.bot.loop.create_task(self.sync())
        self.batch_remove_task = self.bot.loop.create_task(self.batch_member_remove())
        self.bot.loop.create_task(self.sync())


    def cog_unload(self):
        logger.info('die')
        utcnow = datetime.datetime.utcnow()
        self.post_avy_task.cancel()
        self.dl_avys_task.cancel()
        self.batch_remove_task.cancel()
        for recordtype, task in self.bg_tasks.items():
            logger.info(f'canceling {recordtype}')
            task.cancel()
        self.bot.loop.create_task(self.cog_log(False, utcnow))

    async def cog_log(self, start, time):
        event = 'cog_online' if start else 'cog_offline'
        query = '''insert into cog_log (event, time) values ($1, $2)'''
        await self.bot.pool.execute(query, event, time)

    async def batching_task(self, recordtype, interval : int = 5):
        logger.info(f'started {recordtype} task')
        try:
            interval = min(max(1,interval),60)
            await self.bot.wait_until_ready()
            while True:
                await asyncio.sleep(interval)
                await self.insert_to_db(recordtype)
        except asyncio.CancelledError:
            logger.warning(f'Batching task for {recordtype} was cancelled')
            await self.insert_to_db(recordtype)
            if self.bot.pending_updates[recordtype]:
                logger.error(f'{len(self.bot.pending_updates[recordtype])} status updates DIED')
        logger.info(f'exited {recordtype} task')

    async def insert_to_db(self, recordtype):
        to_insert = self.bot.pending_updates[recordtype][:]
        if len(to_insert) == 0:
            return
        self.bot.pending_updates[recordtype] = []
        async with self.bot.pool.acquire() as con:
            result = await con.copy_records_to_table(recordtype, records=to_insert, columns=scheme[recordtype].keys(),schema_name='koi_test')
            if len(to_insert) > 20000 and recordtype != 'statuses':
                key = scheme2[recordtype]['key']
                value = scheme2[recordtype]['value']
                query = f'''
                    delete from
                        {recordtype}
                    where
                        ref in (
                            select
                                ref
                            from (
                                select
                                    ref,
                                    {value},
                                    lead({value}) over (partition by {key} order by first_seen desc) as r_last,
                                    first_seen
                                from {recordtype}
                                order by first_seen desc
                            ) subtable
                            where
                                {value} = r_last
                        )
                '''
                await con.execute(query)

    async def insert_to_db_2(self, recordtype):
        to_insert = self.bot.pending_updates[recordtype][:]
        if len(to_insert) == 0:
            return
        self.bot.pending_updates[recordtype] = []
        names = scheme[recordtype].keys()
        cols = ', '.join(names)
        types = ', '.join(f'{k} {v}' for k, v in scheme[recordtype].items())
        transformed = [{col : row[i] for i, col in enumerate(names)} for row in to_insert]
        query = f'''
                insert into {recordtype} ({cols})
                select {cols}
                from jsonb_to_recordset($1::jsonb) as x({types})
                '''
        await self.bot.pool.execute(query, transformed)

    async def sync(self):
        if self.bot.synced.is_set():
            return # Already successfully synced the bot after initial boot
        await self.bot.wait_until_ready()

        utcnow = datetime.datetime.utcnow()
        await self.bot.request_offline_members(*[guild for guild in self.bot.guilds if guild.large])
        await self.cog_log(True, utcnow - datetime.timedelta(microseconds=1))
        self.add_bulk_members(list(self.bot.get_all_members()), utcnow)
        self.bot.synced.set()
        logger.info("synced!")

    def add_bulk_members(self, members, utcnow):
        for m in members:
            self.bot.pending_updates['nicks'].append((m.id, m.guild.id, m.nick, utcnow))
        logger.info(f'Added members in bulk: {len(set(members))}')
        for m in set(members):
            self.bot.pending_updates['names'].append((m.id, m.name, utcnow))
            self.bot.pending_updates['discrims'].append((m.id, m.discriminator, utcnow))
            self.bot.pending_updates['statuses'].append((m.id, m.status.name, utcnow))


    def add_member(self, m, utcnow, full = True):
        self.bot.pending_updates['nicks'].append((m.id, m.guild.id, m.nick, utcnow))
        if full:
            self.bot.pending_updates['names'].append((m.id, m.name, utcnow))
            self.bot.pending_updates['discrims'].append((m.id, m.discriminator, utcnow))
            self.bot.pending_updates['statuses'].append((m.id, m.status.name, utcnow))

    def fill_updates(self, uid, sid, msg, utcnow, full = True):
        logger.debug(f'running fill_updates with {full}')
        self.bot.pending_updates['nicks'].append((uid, sid, msg, utcnow))
        if full:
            self.bot.pending_removes.append((uid, utcnow))

    async def batch_member_remove(self):
        logger.info('started batch member remove task')
        try:
            await self.bot.wait_until_ready()
            while True:
                await asyncio.sleep(5)
                await self.insert_member_removes()
        except asyncio.CancelledError:
            logger.warning('task for batch member remove was cancelled')
            await self.insert_member_removes()
        logger.info('exited batch member remove task')

    async def insert_member_removes(self):
        to_insert = self.bot.pending_removes[:]
        if len(to_insert) == 0:
            return
        self.bot.pending_removes = []
        transformed = [{'uid' : row[0], 'time' : row[1]} for row in to_insert]
        query = f'''
                insert into member_removes (uid, time)
                select uid, time
                from jsonb_to_recordset($1::jsonb) as x(uid BIGINT, time TIMESTAMP WITHOUT TIME ZONE)
                '''
        await self.bot.pool.execute(query, transformed)

    @commands.Cog.listener()
    async def on_member_join(self, member):
        await self.bot.synced.wait()
        utcnow = datetime.datetime.utcnow()
        do_full = sum(1 for g in self.bot.guilds if g.get_member(member.id)) == 1
        self.add_member(member, utcnow, do_full)

    @commands.Cog.listener()
    async def on_member_remove(self, member):
        await self.bot.synced.wait()
        utcnow = datetime.datetime.utcnow()
        do_full = sum(1 for g in self.bot.guilds if g.get_member(member.id)) == 0
        self.fill_updates(member.id, member.guild.id, 'left_guild', utcnow, do_full) #untested stuff
    

    @commands.Cog.listener()
    async def on_user_update(self, before, after):
        await self.bot.synced.wait()
        utcnow = datetime.datetime.utcnow()
        aid = after.id

        if before.name != after.name:
            self.bot.pending_updates['names'].append((aid, after.name, utcnow))
        if before.discriminator != after.discriminator:
            self.bot.pending_updates['discrims'].append((aid, after.discriminator, utcnow))

    @commands.Cog.listener()
    async def on_member_update(self, before, after):
        await self.bot.synced.wait()
        utcnow = datetime.datetime.utcnow()
        aid = after.id

        if before.nick != after.nick:
            self.bot.pending_updates['nicks'].append((aid, after.guild.id, after.nick, utcnow))

        lowest = discord.utils.find(lambda x: x.get_member(aid) is not None, sorted(self.bot.guilds, key=lambda x: x.id)) # stolen from luma I think
        
        if after.guild.id == lowest.id and before.status != after.status:
            self.bot.pending_updates['statuses'].append((aid, after.status.name, utcnow))

    @commands.Cog.listener()
    async def on_guild_join(self, guild):
        """
            It is rare to have too many dups of members in new guilds.
            Regardless, dups don't matter and are easy to deal with.
        """
        await self.bot.synced.wait()
        utcnow = datetime.datetime.utcnow()
        self.add_bulk_members(guild.members, utcnow)
        logger.info(f'Added {guild.member_count} people to queues!')

    @commands.Cog.listener()
    async def on_guild_remove(self, guild):
        """
            Figuring out which users the bot can still see is important.
            Need to find a better way to figure out if the user is in any other mutual guilds.
        """
        await self.bot.synced.wait()
        utcnow = datetime.datetime.utcnow()
        for member in guild.members:
            if sum(1 for g in self.bot.guilds if g.get_member(member.id)) == 0:
                self.fill_updates(member.id, member.guild.id, 'left_guild', utcnow, True)
            else:
                self.fill_updates(member.id, member.guild.id, 'left_guild', utcnow, False)


def setup(bot):
    if not hasattr(bot, 'pending_updates'):
        bot.pending_updates = {recordtype : [] for recordtype in scheme.keys()}
    if not hasattr(bot, 'pending_removes'):
        bot.pending_removes = []
    if not hasattr(bot, 'avy_posting_queue'):
        bot.avy_posting_queue = asyncio.Queue(maxsize = 50)
    if not hasattr(bot, 'synced'):
        bot.synced = asyncio.Event()

    bot.add_cog(Pop(bot))
