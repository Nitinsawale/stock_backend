from fastapi import APIRouter
from server.service.strategy_service import generate_strategy_data

router = APIRouter()


@router.get("/")
def get_strategy_list():

    data = [{
        "name":"supertrend_with_ma",
        "view_value":"Super Trend With Moving Average"
    }]

    return data

@router.get('/{strategy_name}')
async def get_stocks_for_strategy(strategy_name):
    print(strategy_name)
    return await generate_strategy_data(strategy_name=strategy_name)
    

# @router.get('/strategy/{strategy_name}/{ticker_name}')
# def get_strategy_name(strategy_type, ticker_name):

#     data = generate_strategy_data(strategy_type , ticker_name )
#     return data


