import asyncio
from aiohttp import ClientSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy import Column, Integer, String
from more_itertools import chunked


PG_DSN = 'postgresql+asyncpg://postgres:admin@localhost:5432/star_db'
engine = create_async_engine(PG_DSN)

Base = declarative_base()
Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class People(Base):
    __tablename__ = 'people'

    id = Column(Integer, primary_key=True)
    birth_year = Column(String)
    eye_color = Column(String)
    films = Column(String)
    gender = Column(String)
    hair_color = Column(String)
    height = Column(String)
    homeworld = Column(String)
    mass = Column(String)
    name = Column(String)
    skin_color = Column(String)
    species = Column(String)
    starships = Column(String)
    vehicles = Column(String)


CHUNK_SIZE = 10


async def achunked(async_iter, size):
    buffer = []
    while True:
        try:
            item = await async_iter.__anext__()
        except StopAsyncIteration:
            if buffer:
                yield buffer
            break
        buffer.append(item)
        if len(buffer) == size:
            yield buffer
            buffer = []


async def get_data_from_urls(session, urls):
    results = []
    for url in urls:
        async with session.get(url) as response:
            result = await response.json()
        results.append(result["title" if "title" in result else "name"])
    return ", ".join(results)


async def get_person(people_id: int, session: ClientSession):
    print(f'begin {people_id}')
    async with session.get(f'https://swapi.dev/api/people/{people_id}') as response:
        json_data = await response.json()
    print(f'end {people_id}')

    if "detail" in json_data and json_data["detail"] == "Not found":
        return None

    films = await get_data_from_urls(session, json_data["films"])
    species = await get_data_from_urls(session, json_data["species"])
    starships = await get_data_from_urls(session, json_data["starships"])
    vehicles = await get_data_from_urls(session, json_data["vehicles"])

    return People(
        id=people_id,
        birth_year=json_data["birth_year"],
        eye_color=json_data["eye_color"],
        films=films,
        gender=json_data["gender"],
        hair_color=json_data["hair_color"],
        height=json_data["height"],
        homeworld=json_data["homeworld"].split("/")[-2],
        mass=json_data["mass"],
        name=json_data["name"],
        skin_color=json_data["skin_color"],
        species=species,
        starships=starships,
        vehicles=vehicles,
    )


async def get_people():
    async with ClientSession() as session:
        for chunk in chunked(range(1, 84), CHUNK_SIZE):
            coroutines = [get_person(people_id=i, session=session) for i in chunk]
            results = await asyncio.gather(*coroutines)
            for item in results:
                yield item


async def insert_people(people_chunk):
    async with Session() as session:
        session.add_all(people_chunk)
        await session.commit()


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()

    async for people_chunk in achunked(get_people(), CHUNK_SIZE):
        people_chunk_list = [person for person in people_chunk if person is not None]
        await insert_people(people_chunk_list)

    print("All people data has been inserted into the database.")

    tasks = set(asyncio.all_tasks()) - {asyncio.current_task()}
    for task in tasks:
        await task

if __name__ == "__main__":
    asyncio.run(main())
