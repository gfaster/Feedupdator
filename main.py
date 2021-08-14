from dotenv import dotenv_values
import discord
from discord.ext import commands
from databases import Database
from sqlite3 import IntegrityError
import re
import feedparser
from datetime import datetime as dt
from datetime import timedelta
import asyncio

config = dotenv_values('.env')
database = Database("sqlite:///{0}".format(config['DATABASE']))


bot = commands.Bot(command_prefix='$wm ')


async def db_setup():
	await database.connect()
	q1 = "CREATE TABLE IF NOT EXISTS ChannelFollows (id INTEGER PRIMARY KEY NOT null, channel INTEGER , keyword VARCHAR(1023));"
	q2 = "CREATE INDEX IF NOT EXISTS idx_keyword ON ChannelFollows (keyword);"
	q3 = "CREATE UNIQUE INDEX IF NOT EXISTS idx_channel_keyword ON ChannelFollows (channel, keyword);"
	q4 = "CREATE TABLE IF NOT EXISTS Refresh (id INTEGER PRIMARY KEY NOT null, provider VARCHAR(255), last_refresh DATETIME, etag VARCHAR(255), modified VARCHAR(255))"
	q5 = "CREATE TABLE IF NOT EXISTS PrevSends (id INTEGER PRIMARY KEY NOT null, channel INTEGER, permalink VARCHAR(1023))"
	q6 = "CREATE UNIQUE INDEX IF NOT EXISTS idx_channel_permalink ON PrevSends (channel, permalink);"
	q6 = "CREATE UNIQUE INDEX IF NOT EXISTS idx_provider ON Refresh (provider);"


	queries = [q1, q2, q3, q4, q5, q6]
	for query in queries:
		await database.execute(query=query)
	
	



@bot.event
async def on_ready():
	await db_setup()
	print('We have logged in as {0.user}'.format(bot))

def san(text):
	return re.sub(r'[()";]', '', text)


async def _add(channel_id, keyword):
	pass

@bot.command(help='add a follow for this channel')
async def add(ctx, *args):
	keyword = san(' '.join(args))
	if (len(keyword) >= 1024):
		await ctx.send('too long!')
		return

	query = "INSERT INTO ChannelFollows(channel, keyword) VALUES (:channel, :keyword)"
	values = {'channel': ctx.channel.id, 'keyword':keyword}
	try:
		await database.execute(query=query, values=values)
		await ctx.send('added **{0}** to list of followed titles'.format(keyword))
	except IntegrityError as e:
		await ctx.send('**{0}** is already followed'.format(keyword))
	

@bot.command(help='remove a follow for this channel')
async def remove(ctx, *args):
	keyword = san(' '.join(args))
	if (len(keyword) >= 1024):
		await ctx.send('too long!')
		return

	query = "SELECT * FROM ChannelFollows WHERE channel=:channel AND keyword=:keyword LIMIT 1"
	values = {'channel': ctx.channel.id, 'keyword':keyword}
	exists = await database.fetch_all(query=query, values=values)
	if len(exists) == 0:
		await ctx.send('**{0}** was never followed'.format(keyword))
		return

	query = "DELETE FROM ChannelFollows WHERE channel=:channel AND keyword=:keyword"
	
	await database.execute(query=query, values=values)
	await ctx.send('removed **{0}**'.format(keyword))


@bot.command(help='Clear all follows for this channel')
async def clear(ctx):
	query = "DELETE FROM ChannelFollows WHERE channel=:channel"
	values = {'channel': ctx.channel.id}
	await database.execute(query=query, values=values)
	await ctx.send('cleared all follows for <#{0}>'.format(ctx.channel.id))


@bot.command(help='List all follows for this channel')
async def list(ctx):
	query = """SELECT keyword FROM ChannelFollows WHERE channel=:channel LIMIT 100"""
	values = {'channel': ctx.channel.id}
	co_keywords = await database.fetch_all(query=query, values=values)
	str_keywords = '\n'.join([x[0] for x in co_keywords])
	await ctx.send('followed tags for <#{0}>:\n{1}'.format( str(ctx.channel.id), str_keywords ))

# returns a feed and updates db if enough time has passed since last retry
async def updateRefresh(provider):
	query = "SELECT * FROM Refresh WHERE (provider=:provider)"
	values = {'provider': provider}
	result = await database.fetch_all(query=query, values=values)
	
	

	# has never been refreshed
	if len(result) == 0:
		query = "INSERT INTO Refresh(provider, last_refresh, etag, modified) VALUES (:provider, datetime('now'), :etag, :modified)"
		values = {"provider": provider, "etag": None, "modified": None}
		await database.execute(query=query, values=values)
		return await updateRefresh(provider)

	last_refresh = result[0]

	# make sure it's been a bit, timedelta for EST
	prev_check_time = dt.strptime(last_refresh[2], "%Y-%m-%d %H:%M:%S") - timedelta(hours = 4)
	time_elapsed = (dt.now() - prev_check_time).total_seconds()
	
	if time_elapsed < int(config['REFRESH_SLEEP']):
		return None


	d = feedparser.parse(provider, etag=last_refresh[3], modified=last_refresh[4])
	if int(d.status) == 200:
		etag = getattr(d, 'etag', None)
		modified = getattr(d, 'modified', None)

		query = "UPDATE Refresh SET (last_refresh, etag, modified) = (datetime('now'), :etag, :modified) WHERE provider=:provider"
		values = {"etag": etag, 'modified': modified, 'provider':provider}
		await database.execute(query=query, values=values)

		return d
	else:
		# TODO: Make a cache here
		return None

async def hasSent(channel, permalink):
	query = "SELECT * FROM PrevSends WHERE (channel, permalink) = (:channel, :permalink) LIMIT 1"
	values = {'channel': channel, 'permalink': permalink}
	exists = await database.fetch_all(query=query, values=values)
	return len(exists) != 0

async def sendNewShows(feed):
	for item in feed.entries:

		query = "SELECT * FROM ChannelFollows WHERE keyword=:keyword"
		values = {'keyword':item['crunchyroll_seriestitle']}
		rows_to_send = await database.fetch_all(query=query, values=values)
		for row in rows_to_send:
			if not await hasSent(row[1], item['feedburner_origlink']):
				channel = bot.get_channel(row[1])

				title = '**{0}** is now out!'.format(item['title'])
				embed = discord.Embed(title=title, url=item['feedburner_origlink'])
				embed.set_image(url=item['media_thumbnail'][0]['url'])
				await channel.send(embed=embed)

				query = "INSERT INTO PrevSends(channel, permalink) VALUES (:channel, :permalink)"
				values = {'channel': row[1], 'permalink':item['feedburner_origlink']}
				await database.execute(query=query, values=values)





async def refresh(ctx):

	feed = await updateRefresh('http://feeds.feedburner.com/crunchyroll/rss?format=xml')

	if feed is not None:
		# await ctx.send('Updated!')
		await sendNewShows(feed)
		print(str(dt.now()) + " Updated, send some new shows")
	else:
		print(str(dt.now()) + " Updated, found nothing")
		# await ctx.send('Nothing new :(')

@bot.command(help='where did I come from?')
async def github(ctx):
	await ctx.send(embed="https://github.com/gfaster/Feedupdator")


async def updatePeriodic():
	while True:
		await refresh(bot.get_channel(int(config['UPDATE_CHANNEL'])))
		await asyncio.sleep(int(config['REFRESH_SLEEP']))

async def main():
	await updatePeriodic()
	await bot.run(config['TOKEN'])

if __name__ == '__main__':
	asyncio.run(main())

