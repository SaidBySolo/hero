from dataclasses import asdict
from typing import Any

from sanic import Request, Sanic, json

from hero import Target


class Response(dict[str, Any]):
    def to_json_response(self):
        return json(self, content_type="text/html;charset=UTF-8")

    @classmethod
    def from_data(cls, data: list[Any]):
        d = cls()
        d.update({"hero": [{}, {"row": [asdict(d) for d in data]}]})
        return d.to_json_response()

    @classmethod
    def not_found(cls):
        d = cls()
        d.update({"RESULT": {"CODE": "INFO-200", "MESSAGE": "해당하는 데이터가 없습니다."}})
        return d.to_json_response()


async def handle_timetable(args: dict[str, Any], target: Target):
    edu_office_code = args.get("ATPT_OFCDC_SC_CODE")
    standard_school_code = args.get("SD_SCHUL_CODE")
    ay = args.get("AY", "")
    sem = args.get("SEM", "")
    grade = args.get("GRADE", "")
    room = args.get("CLASS_NM", "")
    date = args.get("ALL_TI_YMD", "")
    if not edu_office_code or not standard_school_code:
        return Response.not_found()

    data = await target.get_timetable(
        edu_office_code, standard_school_code, ay, sem, grade, room, date
    )

    if not data:
        return Response.not_found()

    return Response.from_data(list(data))


app = Sanic(__name__)


@app.before_server_start
async def setup(app: Sanic):
    app.ctx.target = Target.setup("sqlite+aiosqlite:///hero.db")


@app.get("/mealServiceDietInfo")
async def mealServiceDietInfo(request: Request):
    request_args = request.args
    atpt_ofcdc_sc_code = request_args.get("ATPT_OFCDC_SC_CODE")
    sd_schul_code = request_args.get("SD_SCHUL_CODE")
    mlsv_ymd = request_args.get("MLSV_YMD")

    if not atpt_ofcdc_sc_code or not sd_schul_code:
        return Response.not_found()

    data = await app.ctx.target.get_meal(atpt_ofcdc_sc_code, sd_schul_code, mlsv_ymd)

    if not data:
        return Response.not_found()

    return Response.from_data(data)


@app.get("/elsTimetable")
async def elsTimetable(request: Request):
    return await handle_timetable(request.args, app.ctx.target)


@app.get("misTimetable")
async def misTimetable(request: Request):
    return await handle_timetable(request.args, app.ctx.target)


@app.get("/hisTimetable")
async def hisTimetable(request: Request):
    return await handle_timetable(request.args, app.ctx.target)


if __name__ == "__main__":
    app.run("127.0.0.1", dev=True)
