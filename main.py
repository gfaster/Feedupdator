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
from fuzzy_match import algorithims
from dataclasses import dataclass

config = dotenv_values('.env')
database = Database("sqlite:///{0}".format(config['DATABASE']))

command_prefix = '$wm '
bot = commands.Bot(command_prefix=command_prefix)


async def db_setup():
	await database.connect()
	q8 = "CREATE TABLE IF NOT EXISTS Series (id INTEGER PRIMARY KEY NOT null, full_title VARCHAR(1023), platform VARCHAR(255))"
	q1 = """CREATE TABLE IF NOT EXISTS ChannelFollows (
			id INTEGER PRIMARY KEY NOT null, channel INTEGER , series_id INTEGER, FOREIGN KEY(series_id) REFERENCES Series(id));"""
	q2 = "CREATE INDEX IF NOT EXISTS idx_series_id ON ChannelFollows (series_id);"
	q3 = "CREATE UNIQUE INDEX IF NOT EXISTS idx_channel_series_id ON ChannelFollows (channel, series_id);"
	q4 = "CREATE TABLE IF NOT EXISTS Refresh (id INTEGER PRIMARY KEY NOT null, provider VARCHAR(255), last_refresh DATETIME, etag VARCHAR(255), modified VARCHAR(255))"
	q5 = "CREATE TABLE IF NOT EXISTS PrevSends (id INTEGER PRIMARY KEY NOT null, channel INTEGER, permalink VARCHAR(1023))"
	q6 = "CREATE UNIQUE INDEX IF NOT EXISTS idx_channel_permalink ON PrevSends (channel, permalink);"
	q7 = "CREATE UNIQUE INDEX IF NOT EXISTS idx_provider ON Refresh (provider);"





	queries = [q8, q1, q2, q3, q4, q5, q6, q7]
	for query in queries:
		await database.execute(query=query)
	
	



@bot.event
async def on_ready():
	await db_setup()
	print('We have logged in as {0.user}'.format(bot))

def san(text):
	return re.sub(r'[()";]', '', text)


@bot.command(help='add a follow for this channel')
async def add(ctx, *args):
	keyword = san(' '.join(args))
	if (len(keyword) >= 1024):
		await ctx.send('too long!')
		return

	series_match = await findSeries(keyword)

	if series_match.distance > 0.8:
		query = "INSERT INTO ChannelFollows(channel, series_id) VALUES (:channel, :series_id)"
		values = {'channel': ctx.channel.id, 'series_id':series_match.index}
		try:
			await database.execute(query=query, values=values)
			await ctx.send('added **{0}** to list of followed titles'.format(series_match.title))
		except IntegrityError as e:
			await ctx.send('**{0}** is already followed'.format(series_match.title))
	else:
		await ctx.send("couldn't find **{0}**, did you mean **{1}**?".format(keyword, series_match.title))
	

@bot.command(help='remove a follow for this channel')
async def remove(ctx, *args):
	keyword = san(' '.join(args))
	if (len(keyword) >= 1024):
		await ctx.send('too long!')
		return

	series_match = await findSeries(keyword)
	if series_match.distance > 0.8:
		query = "SELECT * FROM ChannelFollows WHERE channel=:channel AND series_id=:series_id LIMIT 1"
		values = {'channel': ctx.channel.id, 'series_id':series_match.index}
		exists = await database.fetch_all(query=query, values=values)
		if len(exists) == 0:
			await ctx.send('**{0}** was never followed, type `{1} list` to see a list of followed series '.format(keyword, command_prefix))
			return

		query = "DELETE FROM ChannelFollows WHERE channel=:channel AND series_id=:series_id"
		
		await database.execute(query=query, values=values)
		await ctx.send('removed **{0}**'.format(series.title))
	else:
		await ctx.send("couldn't find **{0}**, did you mean **{1}**?".format(keyword, series_match.title))


@bot.command(help='Clear all follows for this channel')
async def clear(ctx):
	query = "DELETE FROM ChannelFollows WHERE channel=:channel"
	values = {'channel': ctx.channel.id}
	await database.execute(query=query, values=values)
	await ctx.send('cleared all follows for <#{0}>'.format(ctx.channel.id))


@bot.command(help='List all follows for this channel')
async def list(ctx):
	query = """SELECT Series.full_title 
	FROM Series 
	INNER JOIN ChannelFollows ON Series.id=ChannelFollows.series_id
	WHERE ChannelFollows.channel=:channel LIMIT 100"""

	values = {'channel': ctx.channel.id}

	co_keywords = await database.fetch_all(query=query, values=values)
	str_keywords = '\n'.join([x[0] for x in co_keywords])
	await ctx.send('followed series for <#{0}>:\n{1}'.format( str(ctx.channel.id), str_keywords ))



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
	
	if time_elapsed < int(config['REFRESH_SLEEP']) - 3:
		print('just tried to refresh, but not enough time has passed')
		return None


	# Get a new feed and update the database

	# d = feedparser.parse(provider, etag=last_refresh[3], modified=last_refresh[4])
	d = feedparser.parse(provider, modified=last_refresh[4])
	if int(d.status) == 200:
		etag = getattr(d, 'etag', None)
		modified = getattr(d, 'modified', None)

		query = "UPDATE Refresh SET (last_refresh, etag, modified) = (datetime('now'), :etag, :modified) WHERE provider=:provider"
		values = {"etag": etag, 'modified': modified, 'provider':provider}
		await database.execute(query=query, values=values)

		# update series if there are any
		for entry in d.entries:
			assert 'crunchyroll_seriestitle' in entry
			series_match = await findSeries(entry['crunchyroll_seriestitle'])
			if series_match.distance < 0.95:
				# this is a different series, lets add it
				query = "INSERT INTO Series(full_title, platform) VALUES (:full_title, 'Crunchyroll')"
				values = {'full_title': entry['crunchyroll_seriestitle']}
				await database.execute(query=query, values=values)
				print('\tJust added {0} to the series list'.format(entry['crunchyroll_seriestitle']))



		return d
	else:
		query = "UPDATE Refresh SET last_refresh = datetime('now') WHERE provider=:provider"
		values = {'provider':provider}
		await database.execute(query=query, values=values)
		return None

async def hasSent(channel, permalink):
	query = "SELECT * FROM PrevSends WHERE (channel, permalink) = (:channel, :permalink) LIMIT 1"
	values = {'channel': channel, 'permalink': permalink}
	exists = await database.fetch_all(query=query, values=values)
	return len(exists) != 0

async def sendNewShows(feed):
	for item in feed.entries:

		current_show = await findSeries(item['crunchyroll_seriestitle'])
		assert current_show.index > 0.99
		query = "SELECT * FROM ChannelFollows WHERE series_id=:series_id"
		values = {'series_id':current_show.index}
		followed_rows = await database.fetch_all(query=query, values=values)

		for row in followed_rows:
			if not await hasSent(row[1], item['feedburner_origlink']):
				channel = bot.get_channel(row[1])
				print('\tsending {0} to #{1}'.format(item['title'], channel.name))

				title = '**{0}** is now out!'.format(item['title'])
				embed = discord.Embed(title=title, url=item['feedburner_origlink'])
				embed.set_image(url=item['media_thumbnail'][0]['url'])
				await channel.send(embed=embed)

				query = "INSERT INTO PrevSends(channel, permalink) VALUES (:channel, :permalink)"
				values = {'channel': row[1], 'permalink':item['feedburner_origlink']}
				await database.execute(query=query, values=values)


@dataclass
class Match:
	index: int
	distance: float
	title: str

#fuzzy searches the series table to find a show, returns a Match object for the best match
async def findSeries(series_name):
	# See if there is an exact match
	query = "SELECT * FROM Series WHERE full_title=:series_name"
	values = {'series_name': series_name}
	exact = await database.fetch_all(query=query, values=values)
	if len(exact) > 0:
		return Match(exact[0][0], 1.0, series_name)

	# I haven't found a great way of doing this since I can't fuzzy WHERE easily
	all_series = await database.fetch_all(query="SELECT * FROM Series")

	best_match = None

	for series in all_series:
		match_distance = algorithims.levenshtein(series_name.lower(), series[1].lower())
		if best_match is None or best_match.distance < match_distance:
			best_match = Match(series[0], match_distance, series[1])


	return best_match




async def refresh(ctx):

	feed = await updateRefresh('http://feeds.feedburner.com/crunchyroll/rss?format=xml')

	if feed is not None:
		# await ctx.send('Updated!')
		print(str(dt.now()) + " Updated, send some new shows")
		await sendNewShows(feed)
	else:
		print(str(dt.now()) + " Updated, found nothing")
		# await ctx.send('Nothing new :(')

@bot.command(help='where did I come from?')
async def github(ctx):
	await ctx.send(embed="https://github.com/gfaster/Feedupdator")


async def updatePeriodic():
	await bot.wait_until_ready()
	await asyncio.sleep(3)
	while True:
		await refresh(bot.get_channel(int(config['UPDATE_CHANNEL'])))
		await asyncio.sleep(int(config['REFRESH_SLEEP']))


bot.loop.create_task(updatePeriodic())
bot.run(config['TOKEN'])


