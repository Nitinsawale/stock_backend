from fastapi import APIRouter
from server.service.strategy_service import generate_strategy_data

router = APIRouter()


@router.get('/strategy/{strategy_name}/{ticker_name}')
def get_strategy_name(strategy_type, ticker_name):

    data = generate_strategy_data(strategy_type , ticker_name )
    return data


