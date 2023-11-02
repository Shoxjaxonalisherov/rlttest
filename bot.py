from config import TOKEN
from aiogram import Bot, types, executor
from aiogram.dispatcher import Dispatcher
from aiogram.utils import executor
import json
from aiogram import Bot, Dispatcher, types
from agredate import aggregate

API_TOKEN = TOKEN

bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)

@dp.message_handler()
async def aggregate_data(message: types.Message):
    
    try:
        data = json.loads(message.text)
        dt_from = data.get("dt_from")
        dt_upto = data.get("dt_upto")
        group_type = data.get("group_type")

        result = ''
        if dt_from and dt_upto and group_type:
            result = aggregate(dt_from, dt_upto, group_type)
        
        await message.reply(str(result).replace("'", "\"")) # Изначально бот возвращал результат в одинарных кавычках, чтобы избежать возможных проблем решил заменить все на двойные
        
    except Exception:
        pass

if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)
