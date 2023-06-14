import logging
from datetime import datetime, timedelta
from types import SimpleNamespace
from typing import Any, Optional

from neispy.error import DataNotFound
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import Mapped, mapped_column, registry
from sqlalchemy.sql import select

from crenata.database import Database
from crenata.database.schema import *
from crenata.neispy import CrenataNeispy
from crenata.utils.datetime import to_datetime, to_yyyymmdd

logger = logging.getLogger(__name__)


class SafeNamespace(SimpleNamespace):
    def __getattribute__(self, value: Any):
        try:
            return super().__getattribute__(value)
        except AttributeError:
            return ""


reg = registry()


@reg.mapped_as_dataclass(unsafe_hash=True)
class SchoolInfo:
    __tablename__ = "school_info"
    id: Mapped[int] = mapped_column(primary_key=True, init=False)
    ATPT_OFCDC_SC_CODE: Mapped[str] = mapped_column(default="")
    ATPT_OFCDC_SC_NM: Mapped[str] = mapped_column(default="")
    SD_SCHUL_CODE: Mapped[str] = mapped_column(default="")
    SCHUL_NM: Mapped[str] = mapped_column(default="")
    ENG_SCHUL_NM: Mapped[str] = mapped_column(default="")
    SCHUL_KND_SC_NM: Mapped[str] = mapped_column(default="")
    LCTN_SC_NM: Mapped[str] = mapped_column(default="")
    JU_ORG_NM: Mapped[str] = mapped_column(default="")
    FOND_SC_NM: Mapped[str] = mapped_column(default="")
    ORG_RDNZC: Mapped[str] = mapped_column(default="")
    ORG_RDNMA: Mapped[str] = mapped_column(default="")
    ORG_RDNDA: Mapped[str] = mapped_column(default="")
    ORG_TELNO: Mapped[str] = mapped_column(default="")
    HMPG_ADRES: Mapped[str] = mapped_column(default="")
    COEDU_SC_NM: Mapped[str] = mapped_column(default="")
    ORG_FAXNO: Mapped[str] = mapped_column(default="")
    HS_SC_NM: Mapped[str] = mapped_column(default="")
    INDST_SPECL_CCCCL_EXST_YN: Mapped[str] = mapped_column(default="")
    HS_GNRL_BUSNS_SC_NM: Mapped[str] = mapped_column(default="")
    SPCLY_PURPS_HS_ORD_NM: Mapped[str] = mapped_column(default="")
    ENE_BFE_SEHF_SC_NM: Mapped[str] = mapped_column(default="")
    DGHT_SC_NM: Mapped[str] = mapped_column(default="")
    FOND_YMD: Mapped[str] = mapped_column(default="")
    FOAS_MEMRD: Mapped[str] = mapped_column(default="")
    LOAD_DTM: Mapped[str] = mapped_column(default="")


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


@reg.mapped_as_dataclass
class Timetable:
    __tablename__ = "timetable"
    id: Mapped[int] = mapped_column(primary_key=True, init=False)
    ATPT_OFCDC_SC_CODE: Mapped[str] = mapped_column(default="")
    SD_SCHUL_CODE: Mapped[str] = mapped_column(default="")
    SCHUL_NM: Mapped[str] = mapped_column(default="")
    AY: Mapped[str] = mapped_column(default="")
    SEM: Mapped[str] = mapped_column(default="")
    ALL_TI_YMD: Mapped[str] = mapped_column(default="")
    DGHT_CRSE_SC_NM: Mapped[str] = mapped_column(default="")
    ORD_SC_NM: Mapped[str] = mapped_column(default="")
    DDDEP_NM: Mapped[str] = mapped_column(default="")
    GRADE: Mapped[str] = mapped_column(default="")
    CLRM_NM: Mapped[str] = mapped_column(default="")
    CLASS_NM: Mapped[str] = mapped_column(default="")
    PERIO: Mapped[str] = mapped_column(default="")
    ITRT_CNTNT: Mapped[str] = mapped_column(default="")
    LOAD_DTM: Mapped[str] = mapped_column(default="")


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
            return [(atpt, sd, sn) for atpt, sd, sn in r.all()]


class Target:
    def __init__(self, engine: AsyncEngine):
        self.engine = engine

    @classmethod
    async def setup(cls, db_url: str):
        engine = create_async_engine(db_url)
        async with engine.begin() as connection:
            await connection.run_sync(reg.metadata.create_all, checkfirst=True)
        return cls(engine)

    async def search_school(self, school_name: str):
        async with AsyncSession(self.engine) as session:
            # Even if the word is included, it is possible to search
            stmt = select(SchoolInfo).where(
                SchoolInfo.SCHUL_NM.like(f"%{school_name}%")
            )
            r = await session.execute(stmt)
            return r.scalars().all()

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

    async def get_timetable(
        self,
        atpt: str,
        sd: str,
        ay: str,
        sem: str,
        grade: str,
        room: str,
        date: Optional[str] = None,
    ):
        async with AsyncSession(self.engine) as session:
            stmt = select(Timetable).where(
                Timetable.ATPT_OFCDC_SC_CODE == atpt,
                Timetable.SD_SCHUL_CODE == sd,
                Timetable.AY == ay,
                Timetable.SEM == sem,
                Timetable.GRADE == grade,
                Timetable.CLASS_NM == room,
            )
            if date:
                stmt = stmt.where(Timetable.ALL_TI_YMD == date)
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

    async def put_timetables(self, timetables: list[Timetable]):
        async with AsyncSession(self.engine) as session:
            async with session.begin():
                session.add_all(timetables)


class HeroNeispy(CrenataNeispy):
    def get_all_week(self, date: datetime):
        return [
            date + timedelta(days=i) for i in range(-date.weekday(), 5 - date.weekday())
        ]

    async def log_school_info(
        self, edu_office_code: str, standard_school_code: str, school_name: str
    ):
        try:
            r = await self.schoolInfo(
                ATPT_OFCDC_SC_CODE=edu_office_code,
                SD_SCHUL_CODE=standard_school_code,
                SCHUL_NM=school_name,
            )
        except DataNotFound:
            logger.warning(
                f"Not found timetable args: {edu_office_code}, {standard_school_code}, {school_name}"
            )
            return
        return r

    async def get_all_school_meal(
        self, edu_office_code: str, standard_school_code: str, date: datetime
    ):
        weekend = self.get_all_week(date)
        meal_infos: list[Any] = []

        aws = [
            self.get_meal(
                edu_office_code=edu_office_code,
                standard_school_code=standard_school_code,
                date=day,
            )
            for day in weekend
        ]
        for coro in asyncio.as_completed(aws):
            try:
                r = await coro
            except DataNotFound:
                logger.warning(
                    f"Not found meal args: {edu_office_code}, {standard_school_code}, {date}"
                )
                continue
            assert r
            for meal in r:
                meal = SafeNamespace(**vars(meal))
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

    async def get_all_school_timetable(
        self,
        edu_office_code: str,
        standard_school_code: str,
        school_name: str,
        date: datetime,
    ):
        timetables: list[Timetable] = []
        weekend = self.get_all_week(date)

        if school_name.endswith("초등학교"):
            coro = self.elsTimetable
        elif school_name.endswith("중학교"):
            coro = self.misTimetable
        elif school_name.endswith("고등학교"):
            coro = self.hisTimetable
        else:
            coro = self.spsTimetable

        ay = date.year if date.month > 2 else date.year - 1
        sem = 1 if date.month > 2 and date.month < 8 else 2

        aws = [
            coro(
                edu_office_code,
                standard_school_code,
                ay,
                sem,
                int(to_yyyymmdd(day)),
            )
            for day in weekend
        ]

        for coro in asyncio.as_completed(aws):
            try:
                r = await coro
            except DataNotFound:
                logger.warning(
                    f"Not found timetable args: {edu_office_code}, {standard_school_code}, {school_name}, {ay}, {sem}, {date}"
                )
                continue

            for timetable in r:
                timetable = SafeNamespace(**vars(timetable))
                timetables.append(
                    Timetable(
                        ATPT_OFCDC_SC_CODE=timetable.ATPT_OFCDC_SC_CODE,
                        SD_SCHUL_CODE=timetable.SD_SCHUL_CODE,
                        SCHUL_NM=timetable.SCHUL_NM,
                        AY=timetable.AY,
                        SEM=timetable.SEM,
                        ALL_TI_YMD=timetable.ALL_TI_YMD,
                        DGHT_CRSE_SC_NM=timetable.DGHT_CRSE_SC_NM,
                        ORD_SC_NM=timetable.ORD_SC_NM,
                        DDDEP_NM=timetable.DDDEP_NM,
                        GRADE=timetable.GRADE,
                        CLRM_NM=timetable.CLRM_NM,
                        CLASS_NM=timetable.CLASS_NM,
                        PERIO=timetable.PERIO,
                        ITRT_CNTNT=timetable.ITRT_CNTNT,
                        LOAD_DTM=timetable.LOAD_DTM,
                    )
                )
        return timetables


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

    async def mirror_school_info(self):
        logger.info("Start mirroring school info")
        logger.info("Getting school info from source...")
        source_school_infos = await self.source.get_all_school_info_from_source()
        infos: list[SchoolInfo] = []
        total = len(source_school_infos)
        logger.info("total school: %d", total)
        aws = [
            self.neispy.log_school_info(atpt, sd, sn)
            for atpt, sd, sn in source_school_infos
        ]
        for i, coro in enumerate(asyncio.as_completed(aws)):
            r = await coro
            logger.info("mirroring %d/%d school", i + 1, total)
            if not r:
                continue
            school_info = r[0]
            infos.append(
                SchoolInfo(
                    ATPT_OFCDC_SC_CODE=school_info.ATPT_OFCDC_SC_CODE,
                    ATPT_OFCDC_SC_NM=school_info.ATPT_OFCDC_SC_NM,
                    SD_SCHUL_CODE=school_info.SD_SCHUL_CODE,
                    SCHUL_NM=school_info.SCHUL_NM,
                    ENG_SCHUL_NM=school_info.ENG_SCHUL_NM,
                    SCHUL_KND_SC_NM=school_info.SCHUL_KND_SC_NM,
                    LCTN_SC_NM=school_info.LCTN_SC_NM,
                    JU_ORG_NM=school_info.JU_ORG_NM,
                    FOND_SC_NM=school_info.FOND_SC_NM,
                    ORG_RDNZC=school_info.ORG_RDNZC,
                    ORG_RDNMA=school_info.ORG_RDNMA,
                    ORG_RDNDA=school_info.ORG_RDNDA,
                    ORG_TELNO=school_info.ORG_TELNO,
                    HMPG_ADRES=school_info.HMPG_ADRES,
                    COEDU_SC_NM=school_info.COEDU_SC_NM,
                    ORG_FAXNO=school_info.ORG_FAXNO,
                    HS_SC_NM=school_info.HS_SC_NM,
                    INDST_SPECL_CCCCL_EXST_YN=school_info.INDST_SPECL_CCCCL_EXST_YN,
                    HS_GNRL_BUSNS_SC_NM=school_info.HS_GNRL_BUSNS_SC_NM,
                    SPCLY_PURPS_HS_ORD_NM=school_info.SPCLY_PURPS_HS_ORD_NM,
                    ENE_BFE_SEHF_SC_NM=school_info.ENE_BFE_SEHF_SC_NM,
                    DGHT_SC_NM=school_info.DGHT_SC_NM,
                    FOND_YMD=school_info.FOND_YMD,
                    FOAS_MEMRD=school_info.FOAS_MEMRD,
                    LOAD_DTM=school_info.LOAD_DTM,
                )
            )
        await self.target.put_school_infos(infos)

    async def mirror_meal(self, schoolinfos: list[SchoolInfo], date: datetime):
        logger.info("Start mirroring meal")
        total = len(schoolinfos)
        logger.info("total school: %d", total)

        for i, school in enumerate(schoolinfos):
            logger.info("mirroring %d/%d school", i + 1, total)
            await self.target.put_meals(
                await self.neispy.get_all_school_meal(
                    school.ATPT_OFCDC_SC_CODE, school.SD_SCHUL_CODE, date
                )
            )

    async def mirror_timetable(self, schoolinfos: list[SchoolInfo], date: datetime):
        logger.info("Start mirroring timetable")
        total = len(schoolinfos)
        logger.info("total school: %d", total)
        for i, school in enumerate(schoolinfos):
            logger.info("mirroring %d/%d school", i + 1, total)
            await self.target.put_timetables(
                await self.neispy.get_all_school_timetable(
                    school.ATPT_OFCDC_SC_CODE,
                    school.SD_SCHUL_CODE,
                    school.SCHUL_NM,
                    date,
                )
            )

    async def mirror(self, date: datetime, school_info_mirror: bool = False):
        logger.info("Start mirroring")
        if school_info_mirror:
            logger.info("Mirroring school info...")
            await self.mirror_school_info()
        schoolinfos = list(await self.target.get_all_school_info_from_target())
        logger.info("Mirroring meals...")
        await self.mirror_meal(schoolinfos, date)
        logger.info("Mirroring timetables...")
        await self.mirror_timetable(schoolinfos, date)
        logger.info("Done")


async def main():
    # start logging
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s][%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    hero = await Hero.setup(
        "sqlite+aiosqlite:///rena.db",
        "sqlite+aiosqlite:///hero.db",
        "",
    )

    try:
        await hero.mirror(to_datetime("20230615"), True)
        await hero.mirror(to_datetime("20230616"))
        await hero.mirror(to_datetime("20230617"))
        await hero.mirror(to_datetime("20230618"))
        await hero.mirror(to_datetime("20230619"))
        await hero.mirror(to_datetime("20230620"))
    finally:
        if hero.neispy.session and not hero.neispy.session.closed:
            await hero.neispy.session.close()


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
