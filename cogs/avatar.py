import asyncio
import datetime
import math
import time
import typing
from io import BytesIO

import aiohttp
import discord
from discord.ext import commands
from PIL import Image

from .utils import images

class Timetracker:
    def __init__(self, title, *values):
        self.title = title
        self.values = values
        self.times = [time.perf_counter()]

    def update(self):
        self.times.append(time.perf_counter())

    def display(self):
        msg = self.title
        for i, value in enumerate(self.values):
            msg += f'\n{value}: {f"{(self.times[i+1] - self.times[i])*1000:.2f}ms" if i+1 < len(self.times) else "..."}'
        return msg

class Avatar(commands.Cog):
    UPLOAD_SIZE_LIMIT = 8_000_000
    QUERY = '''
        select
            avy_urls.url
        from (
            select
                avatar, first_seen
            from (
                select
                    avatar, lag(avatar) over (order by first_seen desc) as avatar_old, first_seen
                from avatars
                where
                    avatars.uid = $1
            ) a
            where
                avatar != avatar_old or avatar_old is null
        ) avys
        left join avy_urls on
            avy_urls.hash = avys.avatar
        order by avys.first_seen desc
        offset $2 rows
        fetch first $3 rows only
     '''

    def __init__(self, bot):
        self.bot = bot

    @commands.command()
    async def avyquilt(self, ctx, member : discord.Member = None):
        member = member or ctx.author

        tracker = Timetracker('', 'queried', 'downloaded', 'created file')
        msg = await ctx.send(tracker.display())

        urls = await ctx.bot.pool.fetch(self.QUERY, member.id, 0, 100)

        tracker.update()
        await msg.edit(content=tracker.display())

        async def url_to_bytes(url):
            if not url:
                return None
            async with ctx.bot.session.get(url) as r:
                return BytesIO(await r.read())

        avys = await asyncio.gather(*[url_to_bytes(url['url']) for url in urls])

        tracker.update()
        await msg.edit(content=tracker.display())

        file = await ctx.bot.loop.run_in_executor(None, self._avyquilt, avys)

        tracker.update()
        await msg.edit(content=tracker.display())

        await ctx.send(file=discord.File(file, f'{member.id}_avyquilt.png'))

    def _avyquilt(self, avatars):
        xbound = math.ceil(math.sqrt(len(avatars)))
        ybound = math.ceil(len(avatars) / xbound)
        size = int(2520 / xbound)

        with Image.new('RGBA', size=(xbound * size, ybound * size), color=(0,0,0,0)) as base:
            x, y = 0, 0
            for avy in avatars:
                if avy:
                    im = Image.open(avy).resize((size,size), resample=Image.BICUBIC)
                    base.paste(im, box=(x * size, y * size))
                if x < xbound - 1:
                    x += 1
                else:
                    x = 0
                    y += 1
            buffer = BytesIO()
            base.save(buffer, 'png')
            buffer.seek(0)
            buffer = images.resize_to_limit(buffer, self.UPLOAD_SIZE_LIMIT)
            return buffer

    @commands.command()
    async def avyold(self, ctx, member: typing.Optional[discord.Member] = None, index=1):
        member = member or ctx.author
        if index < 1:
            await ctx.send('Index must be ≥1.')
            return
        offset = index - 1
        url = await self.bot.pool.fetchval(self.QUERY, member.id, offset, 1)
        if url is None:
            await ctx.send('Avatar not found.')
            return

        avy = None
        try:
            async with self.bot.session.get(url) as r:
                if r.status in range(200, 300):
                    avy = BytesIO(await r.read())
        except (asyncio.TimeoutError, aiohttp.ClientError):
            pass

        if avy is None:
            await ctx.send('Error downloading avatar.')
            return

        avy = await self.bot.loop.run_in_executor(None, self.resize, avy)
        await ctx.send(file=discord.File(avy, f'{member.id}_avyold_{index}.png'))

    @classmethod
    def resize(cls, avy):
        im = Image.open(avy).resize((200, 200), resample=Image.BICUBIC)
        out = BytesIO()
        im.save(out, 'png')
        out.seek(0)
        return images.resize_to_limit(out, cls.UPLOAD_SIZE_LIMIT)

def setup(bot):
    bot.add_cog(Avatar(bot))
