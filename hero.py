from datetime import datetime, timedelta
from typing import Any, Optional

from neispy.utils import KST
from crenata.utils.datetime import to_datetime
from neispy.error import DataNotFound
from sqlalchemy.ext.asyncio import AsyncSession, AsyncEngine, create_async_engine
from sqlalchemy.orm import Mapped, mapped_column, registry
from sqlalchemy.sql import select

from crenata.database import Database
from crenata.database.schema import *
from crenata.neispy import CrenataNeispy

reg = registry()


@reg.mapped_as_dataclass(unsafe_hash=True)
class SchoolInfo:
    __tablename__ = "school_info"
    id: Mapped[int] = mapped_column(primary_key=True, init=False)
    ATPT_OFCDC_SC_CODE: Mapped[str] = mapped_column()
    SD_SCHUL_CODE: Mapped[str] = mapped_column()
    school_name: Mapped[str] = mapped_column(default=None)


@reg.mapped_as_dataclass
class Meal:
    __tablename__ = "meal"
    id: Mapped[int] = mapped_column(primary_key=True, init=False)
    ATPT_OFCDC_SC_CODE: Mapped[str] = mapped_column(default="")
    SD_SCHUL_CODE: Mapped[str] = mapped_column(default="")
    SCHUL_NM: Mapped[str] = mapped_column(default="")
    MLSV_YMD: Mapped[str] = mapped_column(default="")
    MMEAL_SC_NM: Mapped[str] = mapped_column(default="")
    MLSV_FGR: Mapped[str] = mapped_column(default="")
    DDISH_NM: Mapped[str] = mapped_column(default="")
    ORPLC_INFO: Mapped[str] = mapped_column(default="")
    CAL_INFO: Mapped[str] = mapped_column(default="")
    NTR_INFO: Mapped[str] = mapped_column(default="")
    MLSV_FROM_YMD: Mapped[str] = mapped_column(default="")
    MLSV_TO_YMD: Mapped[str] = mapped_column(default="")


class Source:
    def __init__(self, database: Database):
        self.database = database

    @classmethod
    async def setup(cls, db_url: str):
        return cls(await Database.setup(db_url))

    async def get_all_school_info_from_source(self):
        async with AsyncSession(self.database.engine) as session:
            stmt = (
                select(
                    SchoolInfoSchema.ATPT_OFCDC_SC_CODE,
                    SchoolInfoSchema.SD_SCHUL_CODE,
                    SchoolInfoSchema.school_name,
                )
                # distinct on is supported by postgresql only
                .distinct(
                    SchoolInfoSchema.ATPT_OFCDC_SC_CODE, SchoolInfoSchema.SD_SCHUL_CODE
                ).where(
                    SchoolInfoSchema.ATPT_OFCDC_SC_CODE.is_not(None),
                    SchoolInfoSchema.SD_SCHUL_CODE.is_not(None),
                )
            )
            r = await session.execute(stmt)
            return [SchoolInfo(atpt, sd, sn) for atpt, sd, sn in r.all()]


class Target:
    def __init__(self, engine: AsyncEngine):
        self.engine = engine

    @classmethod
    async def setup(cls, db_url: str):
        engine = create_async_engine(db_url)
        async with engine.begin() as connection:
            await connection.run_sync(reg.metadata.create_all, checkfirst=True)
        return cls(engine)

    async def get_all_school_info_from_target(self):
        async with AsyncSession(self.engine) as session:
            stmt = select(SchoolInfo)
            r = await session.execute(stmt)
            return r.scalars().all()

    async def get_meal(self, atpt: str, sd: str, date: Optional[str] = None):
        async with AsyncSession(self.engine) as session:
            stmt = select(Meal).where(
                Meal.ATPT_OFCDC_SC_CODE == atpt,
                Meal.SD_SCHUL_CODE == sd,
            )
            if date:
                stmt = stmt.where(Meal.MLSV_YMD == date)
            r = await session.execute(stmt)
            return r.scalars().all()

    async def put_school_infos(self, school_infos: list[SchoolInfo]):
        async with AsyncSession(self.engine) as session:
            async with session.begin():
                session.add_all(school_infos)

    async def put_meals(self, meals: list[Meal]):
        async with AsyncSession(self.engine) as session:
            async with session.begin():
                session.add_all(meals)


class HeroNeispy(CrenataNeispy):
    def get_all_week(self, date: datetime):
        return [
            date + timedelta(days=i) for i in range(-date.weekday(), 5 - date.weekday())
        ]

    async def get_weekday_meal(self, school_info: SchoolInfo, date: datetime):
        weekday = self.get_all_week(date)
        meal_infos: list[Any] = []
        for day in weekday:
            try:
                r = await self.get_meal(
                    school_info.ATPT_OFCDC_SC_CODE, school_info.SD_SCHUL_CODE, date=day
                )
            except DataNotFound:
                continue
            assert r
            for meal in r:
                meal_infos.append(
                    Meal(
                        ATPT_OFCDC_SC_CODE=meal.ATPT_OFCDC_SC_CODE,
                        SD_SCHUL_CODE=meal.SD_SCHUL_CODE,
                        MLSV_YMD=meal.MLSV_YMD,
                        MMEAL_SC_NM=meal.MMEAL_SC_NM,
                        MLSV_FGR=meal.MLSV_FGR,
                        DDISH_NM=meal.DDISH_NM,
                        ORPLC_INFO=meal.ORPLC_INFO,
                        CAL_INFO=meal.CAL_INFO,
                        NTR_INFO=meal.NTR_INFO,
                        MLSV_FROM_YMD=meal.MLSV_FROM_YMD,
                        MLSV_TO_YMD=meal.MLSV_TO_YMD,
                    )
                )
        return meal_infos


class Hero:
    def __init__(self, source: Source, target: Target, neispy: HeroNeispy):
        self.source = source
        self.target = target
        self.neispy = neispy

    @classmethod
    async def setup(cls, source_db_url: str, target_db_url: str, neis_api_key: str):
        return cls(
            await Source.setup(source_db_url),
            await Target.setup(target_db_url),
            HeroNeispy(neis_api_key),
        )

    async def compare_school_info(self):
        source_school_infos = await self.source.get_all_school_info_from_source()
        target_school_infos = await self.target.get_all_school_info_from_target()
        source_school_infos = set(source_school_infos)
        target_school_infos = set(target_school_infos)
        return source_school_infos - target_school_infos

    async def mirror_school_info(self):
        school_infos = await self.compare_school_info()
        await self.target.put_school_infos(list(school_infos))


async def main():
    hero = await Hero.setup(
        "sqlite+aiosqlite:///rena.db", "sqlite+aiosqlite:///hero.db", ""
    )
    # # await hero.mirror_school_info()
    # # r = await hero.neispy.get_weekday_meal(
    # #     SchoolInfo("B10", "7091444"), date=datetime.now(KST)
    # # )
    # # # print(r)
    # # await hero.target.put_meals(r)
    # print(await hero.target.get_meal("B10", "7091444", "20230601"))
    # if hero.neispy.session and not hero.neispy.session.closed:
    #     await hero.neispy.session.close()

    hero.neispy.BASE = "http://localhost:8000"
    print(await hero.neispy.mealServiceDietInfo("B10", "7091444", MLSV_YMD="20230601"))


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
