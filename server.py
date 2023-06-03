from dataclasses import asdict
from typing import Any
from sanic import Request, Sanic, json
from hero import Hero


class Response(dict[str, Any]):
    @classmethod
    def from_data(cls, data: list[Any]):
        d = cls()
        d.update({"hero": [{}, {"row": [asdict(d) for d in data]}]})
        return json(d, content_type="text/html;charset=UTF-8")


app = Sanic(__name__)


@app.before_server_start
async def setup(app: Sanic):
    app.ctx.hero = await Hero.setup(
        "sqlite+aiosqlite:///rena.db", "sqlite+aiosqlite:///hero.db", ""
    )


@app.get("/mealServiceDietInfo")
async def mealServiceDietInfo(request: Request):
    request_args = request.args
    atpt_ofcdc_sc_code = request_args.get("ATPT_OFCDC_SC_CODE")
    sd_schul_code = request_args.get("SD_SCHUL_CODE")
    mlsv_ymd = request_args.get("MLSV_YMD")

    if not atpt_ofcdc_sc_code or not sd_schul_code:
        return Response.from_data([])

    data = await app.ctx.hero.target.get_meal(
        atpt_ofcdc_sc_code, sd_schul_code, mlsv_ymd
    )

    return Response.from_data(data)


if __name__ == "__main__":
    app.run("127.0.0.1")
