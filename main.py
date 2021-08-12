from dotenv import dotenv_values
import discord
from discord.ext import commands
from databases import Database

config = dotenv_values('.env')


bot = commands.Bot(command_prefix='$')

@bot.event
async def on_ready():
    print('We have logged in as {0.user}'.format(bot))

@bot.command()
async def add(ctx, *args):
    await ctx.send('added: ' + ' '.join(args))

@bot.command()
async def info(ctx):
	await ctx.send('channel id: '+ctx.channel.id)

bot.run(config['TOKEN'])