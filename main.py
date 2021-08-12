from dotenv import dotenv_values
import discord
from discord.ext import commands
from databases import Database
from sqlite3 import IntegrityError

config = dotenv_values('.env')
database = Database("sqlite:///{0}".format(config['DATABASE']))


bot = commands.Bot(command_prefix='$wm ')


async def db_setup():
	await database.connect()
	q1 = "CREATE TABLE IF NOT EXISTS ChannelFollows (id INTEGER PRIMARY KEY, channel INTEGER , keyword VARCHAR(1023));"
	q2 = "CREATE INDEX IF NOT EXISTS idx_keyword ON ChannelFollows (keyword);"
	q3 = "CREATE UNIQUE INDEX IF NOT EXISTS idx_channel_keyword ON ChannelFollows (channel, keyword);"
	await database.execute(query=q1)
	await database.execute(query=q2)
	await database.execute(query=q3)



@bot.event
async def on_ready():
	await db_setup()
	print('We have logged in as {0.user}'.format(bot))


@bot.command(help='add a follow for this channel')
async def add(ctx, *args):
	keyword = ' '.join(args)
	if (len(keyword) >= 1024):
		await ctx.send('too long!')
		return

	query = "INSERT INTO ChannelFollows(channel, keyword) VALUES (:channel, :keyword)"
	values = {'channel': ctx.channel.id, 'keyword':keyword}
	try:
		await database.execute(query=query, values=values)
		await ctx.send('added **{0}** to list of followed keywords'.format(keyword))
	except IntegrityError as e:
		await ctx.send('**{0}** is already followed'.format(keyword))
	

@bot.command(help='remove a follow for this channel')
async def remove(ctx, *args):
	keyword = ' '.join(args)
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

# @bot.command()
# async def info(ctx):
# 	await ctx.send('channel id: '+ctx.channel.id)


bot.run(config['TOKEN'])