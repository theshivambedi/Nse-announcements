from __future__ import annotations
import os, sys, signal, platform, logging, asyncio, contextlib, pandas as pd
from pathlib import Path
from http import HTTPMethod
from threading import Thread
from curl_cffi import CurlHttpVersion
from datetime import datetime as dtdt
from datetime import date
from logging.handlers import RotatingFileHandler
from dateutil.relativedelta import relativedelta as rtd
from typing import Any, Dict, Optional, NoReturn, Union, Tuple, Literal, List
from curl_cffi.requests import AsyncSession, RequestsError

if sys.platform.startswith("win"):
    from signal import SIGABRT, SIGINT, SIGTERM

    SIGNALS = (SIGABRT, SIGINT, SIGTERM)
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
else:
    from signal import SIGABRT, SIGINT, SIGTERM, SIGHUP

    SIGNALS = (SIGABRT, SIGINT, SIGTERM, SIGHUP)

__all__ = ["NseFetch", "ProgramKilled"]


class ProgramKilled(Exception):
    """ProgramKilled Checks the ProgramKilled exception"""

    pass  # type: ignore


class NseFetch:
    LOGGING_FORMAT: str = "[%(levelname)s]|[%(asctime)s]|[%(name)s::%(module)s::%(funcName)s::%(lineno)d]|=> %(message)s"
    TODAY: str = "Today"
    LAST1WEEK: str = "Last1Week"
    NEXT1WEEK: str = "Next1Week"
    LAST15DAYS: str = "Last15Days"
    NEXT15DAYS: str = "Next15Days"
    LAST1MONTH: str = "Last1Month"
    NEXT1MONTH: str = "Next1Month"
    NEXT3MONTHS: str = "Next3Months"
    LAST3MONTHS: str = "Last3Months"
    LAST6MONTHS: str = "Last6Months"
    LAST1YEAR: str = "Last1Year"
    CUSTOM: str = "Custom"
    ALLFORTHCOMING: str = "AllForthcoming"
    ROOT: str = "https://www.nseindia.com"
    APIBASE: str = "/api"
    ROUTES: Dict[str, str] = {
        "ca": "/corporate-announcements",
        "search": "/search/autocomplete",
    }

    @staticmethod
    def get_route_url(route: str) -> str:
        if route in NseFetch.ROUTES:
            return "".join([NseFetch.ROOT, NseFetch.APIBASE, NseFetch.ROUTES[route]])
        else:
            return "".join([NseFetch.ROOT, NseFetch.APIBASE, route])

    @staticmethod
    def get_now_date_time_with_microseconds_string() -> str:
        return dtdt.now().strftime("%d_%b_%Y_%H_%M_%S_%f")

    @staticmethod
    def is_windows() -> bool:
        return (
            os.name == "nt"
            and sys.platform == "win32"
            and platform.system() == "Windows"
        )

    @staticmethod
    def is_linux() -> bool:
        return (
            os.name == "posix"
            and platform.system() == "Linux"
            and sys.platform in {"linux", "linux2"}
        )

    @staticmethod
    def is_mac() -> bool:
        return (
            os.name == "posix"
            and sys.platform == "darwin"
            and platform.system() == "Darwin"
        )

    @staticmethod
    def get_logger(name, filename, level=logging.WARNING) -> logging.Logger:
        logger = logging.getLogger(name)
        logger.setLevel(level)

        stream = logging.StreamHandler()
        stream.setFormatter(logging.Formatter(NseFetch.LOGGING_FORMAT))
        logger.addHandler(stream)

        fh = RotatingFileHandler(filename, maxBytes=100 * 1024 * 1024, backupCount=25)
        fh.setFormatter(logging.Formatter(NseFetch.LOGGING_FORMAT))
        logger.addHandler(fh)
        logger.propagate = False
        return logger

    @staticmethod
    def start_background_loop(loop: asyncio.AbstractEventLoop) -> Optional[NoReturn]:
        asyncio.set_event_loop(loop)
        try:
            loop.run_forever()
        except (KeyboardInterrupt, SystemExit):
            loop.run_until_complete(loop.shutdown_asyncgens())
            if loop.is_running():
                loop.stop()
            if not loop.is_closed():
                loop.close()

    @staticmethod
    def get_from_to_dates(
        mode: str = ALLFORTHCOMING,
        from_date: Union[date, str, None] = None,
        to_date: Union[date, str, None] = None,
    ) -> Tuple[Union[str, None], Union[str, None]]:
        if mode == NseFetch.ALLFORTHCOMING:
            return None, None
        if (
            mode not in {NseFetch.ALLFORTHCOMING, NseFetch.CUSTOM}
            and from_date is None
            and to_date is None
        ):
            ((from_date, to_date)) = (
                (date.today() - rtd(days=1), date.today())
                if mode == NseFetch.TODAY
                else (date.today(), date.today() + rtd(weeks=1))
                if mode == NseFetch.NEXT1WEEK
                else (date.today() - rtd(weeks=1), date.today())
                if mode == NseFetch.LAST1WEEK
                else (date.today(), date.today() + rtd(days=15))
                if mode == NseFetch.NEXT15DAYS
                else (date.today() - rtd(days=15), date.today())
                if mode == NseFetch.LAST15DAYS
                else (date.today(), date.today() + rtd(months=1))
                if mode == NseFetch.NEXT1MONTH
                else (date.today() - rtd(months=1), date.today())
                if mode == NseFetch.LAST1MONTH
                else (date.today(), date.today() + rtd(months=3))
                if mode == NseFetch.NEXT3MONTHS
                else (date.today() - rtd(months=3), date.today())
                if mode == NseFetch.LAST3MONTHS
                else (date.today() - rtd(months=6), date.today())
                if mode == NseFetch.LAST6MONTHS
                else (date.today() - rtd(years=1), date.today())
            )
            return from_date.strftime("%d-%m-%Y"), to_date.strftime("%d-%m-%Y")
        if mode == NseFetch.CUSTOM and from_date is not None and to_date is not None:
            if isinstance(from_date, date) and isinstance(to_date, date):
                return from_date.strftime("%d-%m-%Y"), to_date.strftime("%d-%m-%Y")
            if isinstance(from_date, str) and isinstance(to_date, str):
                return from_date, to_date
            else:
                raise Exception(
                    "From Date and To Date Can Not Be Empty and Should be of Either Date Type or a String in `dd-mm-YYYY` Format"
                )
        else:
            raise Exception(
                "From Date and To Date Can Not Be Empty"
                + " When Mode is Custom with `from_date` and `to_date`"
            )

    def __aenter__(self) -> "NseFetch":
        return self

    def __enter__(self) -> "NseFetch":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.__graceful_exit()

    def __del__(self) -> None:
        self.__graceful_exit()

    def __delete__(self) -> None:
        self.__graceful_exit()

    def __init__(
        self,
        max_retries: int = 5,
        debug: bool = True,
        debug_verbose: bool = False,
        http_version: CurlHttpVersion = CurlHttpVersion.V2_PRIOR_KNOWLEDGE,
    ) -> None:
        self.max_retries = max_retries
        self.debug = debug
        self.debug_verbose = debug_verbose
        self.http_version = http_version
        if self.debug or self.debug_verbose:
            self.log_level = (
                logging.INFO
                if self.debug
                else logging.DEBUG
                if self.debug_verbose
                else logging.WARNING
            )
            self.logfile = Path.cwd().joinpath(
                f"logs/NseFetch_{NseFetch.get_now_date_time_with_microseconds_string()}.log"
            )
            os.makedirs(self.logfile.parent, exist_ok=True)
            self.log = NseFetch.get_logger(
                "NseFetch", filename=self.logfile, level=self.log_level
            )
            logging.basicConfig(format=NseFetch.LOGGING_FORMAT, level=self.log_level)
        self.__initialize_loop()
        self._initialize_session()

    def __graceful_exit(self) -> None:
        with contextlib.suppress(RuntimeError, RuntimeWarning):
            if hasattr(self, "__reqsession") and self.__reqsession is not None:
                asyncio.run_coroutine_threadsafe(
                    asyncio.ensure_future(self.__reqsession.close()), self.__loop
                )
                asyncio.run_coroutine_threadsafe(asyncio.sleep(0.25), self.__loop)
            asyncio.run_coroutine_threadsafe(
                self.__loop.shutdown_asyncgens(), self.__loop
            )
            if self.__loop.is_running():
                self.__loop.stop()
            if not self.__loop.is_closed():
                self.__loop.close()

    def handle_stop_signals(self, *args, **kwargs):
        try:
            self.__graceful_exit()
        except Exception as err:
            self.log.error(str(err))
        else:
            exit()

    def __initialize_loop(self) -> None:
        self.__loop = asyncio.new_event_loop()
        if NseFetch.is_windows():
            with contextlib.suppress(ValueError):
                for sig in SIGNALS:
                    signal.signal(sig, self.handle_stop_signals)
        else:
            with contextlib.suppress(ValueError):
                for sig in SIGNALS:
                    self.__loop.add_signal_handler(sig, self.handle_stop_signals)
        self._event_thread = Thread(
            target=self.start_background_loop,
            args=(self.__loop,),
            name=f"{self.__class__.__name__}_event_thread",
            daemon=True,
        )
        self._event_thread.start()
        self.log.info("NseFetch Event Loop has been initialized.")

    async def __initialize_session(self, restart: bool = False) -> None:
        if restart and hasattr(self, "__reqsession"):
            self.log.info("Closing Previous Requests Session")
            await self.__reqsession.close()
            await asyncio.sleep(0.25)
        self.log.info("Initializing New Requests Session")
        self.__reqsession = AsyncSession(
            loop=self.__loop,
            verify=True,
            timeout=30,
            http_version=self.http_version,
            impersonate="chrome120",
        )
        retry_no, status_code, respose_text = 0, None, None
        while retry_no < 5:
            try:
                response = await self.__reqsession.get(NseFetch.ROOT + "/")
                status_code = response.status_code
                respose_text = response.text
                response.raise_for_status()
            except RequestsError as err:
                self.log.error(
                    "NSE HomePage Request Failed With Status Code: %d, Response Text: %s",
                    status_code,
                    respose_text,
                )
                self.log.error(
                    "While Fetching NSE Homepage, An Exception: %s Occured", err
                )
                retry_no = +1
                if retry_no == 4:
                    self.log.critical(
                        "Retry Limit Exahusted, Retried %d Times But Failed.", retry_no
                    )
                    self.log.info("Exiting Program")
                    self.__graceful_exit()
                else:
                    self.log.info(
                        "Going To retry after %d second, Retry. no. %d", retry_no, retry_no
                    )
                    await asyncio.sleep(retry_no)
            else:
                self.log.info(
                    "NSE HomePage Request Succeded With Status Code: %d, Response Text: %s...Truncated To 500 Chars.",
                    status_code,
                    respose_text[:500],
                )
                return

    def _initialize_session(self, restart: bool = False) -> None:
        future = asyncio.run_coroutine_threadsafe(
            self.__initialize_session(),
            self.__loop,
        )
        try:
            future.result(5.0)
        except TimeoutError:
            error_message = f"The Initialization of Async Client Session Took Longer Than The Default Timeout To Wait For The Response, i.e. {float(5.0):.2f} Seconds, Cancelling The Task..."
            self.log.error(error_message)
            future.cancel()
        except Exception as exc:
            error_message = f"The Initialization of Async Client Session Ended Up With An Exception: {exc!r} {future.exception(1.0)}"
            self.log.exception(error_message)

    async def __get_issuer(self, query: str) -> Optional[str]:
        data = await self.__get("search", params={"q": query})
        if data is not None and isinstance(data, dict) and "symbols" in data:
            return pd.DataFrame(data["symbols"]).query("symbol == @query")[
                "symbol_info"
            ][0]

    async def __get_corporate_announcement(
        self,
        index: Literal[
            "equities", "sme", "sse", "debt", "municipalBond", "invitsreits", "mf"
        ] = "equities",
        data_for: str = ALLFORTHCOMING,
        symbol: Union[str, None] = None,
        from_date: Union[date, str, None] = None,
        to_date: Union[date, str, None] = None,
    ) -> Optional[Union[pd.DataFrame, str]]:
        params = {"index": index}
        from_date, to_date = NseFetch.get_from_to_dates(data_for, from_date, to_date)
        if symbol is not None:
            issuer = await self.__get_issuer(symbol)
            if issuer is not None:
                params.update(
                    {
                        "symbol": symbol,
                        "issuer": issuer,
                    }
                )
            else:
                self.log.error("Issuer Details for Symbol: %s Not Found", symbol)
                return
        if from_date is not None and to_date is not None:
            params.update({"from_date": from_date, "to_date": to_date})
        data = await self.__get("ca", params=params)
        if data is not None and isinstance(data, list) and isinstance(data[0], dict):
            df = pd.DataFrame.from_records(data)
            df["an_dt"] = pd.to_datetime(df["an_dt"])
            return df.set_index("an_dt").sort_index()
        else:
            msg = (
                "It's Likely That No Events Are There For The Selected Periods\n"
                + "Or nseindia.com Has Not Returned Back Event Calender Data!"
            )
            self.log.error(msg)
            return msg

    def get_corporate_announcement(
        self,
        index: Literal[
            "equities", "sme", "sse", "debt", "municipalBond", "invitsreits", "mf"
        ] = "equities",
        data_for: str = ALLFORTHCOMING,
        symbol: Union[str, None] = None,
        from_date: Union[date, str, None] = None,
        to_date: Union[date, str, None] = None,
    ) -> Optional[Union[pd.DataFrame, str]]:
        _timeout = 30.0 * float(self.max_retries + 1)
        future = asyncio.run_coroutine_threadsafe(
            self.__get_corporate_announcement(),
            self.__loop,
        )
        try:
            result = future.result(_timeout)
        except TimeoutError:
            error_message = f"The Initialization of Async Client Session Took Longer Than The Default Timeout To Wait For The Response, i.e. {_timeout:.2f} Seconds, Cancelling The Task..."
            self.log.error(error_message)
            future.cancel()
        except Exception as exc:
            error_message = f"The Initialization of Async Client Session Ended Up With An Exception: {exc!r} {future.exception(_timeout)}"
            self.log.exception(error_message)
        else:
            return result

    async def __get(
        self, url: str, **kwargs
    ) -> Optional[Union[Any, Dict[str, Any], List[Dict[str, Any]]]]:
        return await self.__request(
            HTTPMethod.GET, NseFetch.get_route_url(url), **kwargs
        )

    async def __post(
        self, url: str, **kwargs
    ) -> Optional[Union[Any, Dict[str, Any], List[Dict[str, Any]]]]:
        return await self.__request(
            HTTPMethod.POST, NseFetch.get_route_url(url), **kwargs
        )

    async def __put(
        self, url: str, **kwargs
    ) -> Optional[Union[Any, Dict[str, Any], List[Dict[str, Any]]]]:
        return await self.__request(
            HTTPMethod.PUT, NseFetch.get_route_url(url), **kwargs
        )

    async def __delete(
        self, url: str, **kwargs
    ) -> Optional[Union[Any, Dict[str, Any], List[Dict[str, Any]]]]:
        return await self.__request(
            HTTPMethod.DELETE, NseFetch.get_route_url(url), **kwargs
        )

    async def __request(
        self, method: HTTPMethod, url: str, **kwargs
    ) -> Optional[Union[Any, Dict[str, Any], List[Dict[str, Any]]]]:
        _locals = locals()
        retry_no, status_code, respose_text = 0, None, None
        while retry_no < self.max_retries:
            try:
                self.log.info(
                    "Initializing Request For Endpoint: %s, Method: %s", url, method
                )
                if "params" in _locals and _locals.get("params") is not None:
                    self.log.info("Request Params: %s", params)
                if "data" in _locals and _locals.get("data") is not None:
                    self.log.info("Request Data: %s", data)
                if "json" in _locals and _locals.get("json") is not None:
                    self.log.info("Request Json: %s", json)
                response = await self.__reqsession.request(method, url, **kwargs)
                status_code = response.status_code
                respose_text = response.text
                response.raise_for_status()
            except RequestsError as err:
                self.log.error(
                    "Method: %s, Request For Endpoint: %s, Failed With Status Code: %d, Response Text: %s",
                    method,
                    url,
                    status_code,
                    respose_text,
                )
                self.log.error(
                    "While Fetching NSE Homepage, An Exception: %s Occured", err
                )
                retry_no = +1
                if retry_no == 4:
                    self.log.critical(
                        "Retry Limit Exahusted, Retried %d Times But Failed.", retry_no
                    )
                    self.log.info("Exiting Program")
                    self.__graceful_exit()
                else:
                    self.log.info(
                        "Going To retry after %d second, Retry. no. %d", retry_no, retry_no
                    )
                    await asyncio.sleep(retry_no)
                    await self.__initialize_session(restart=True)
            else:
                self.log.info(
                    "Method: %s, Request For Endpoint: %s Succeded With Status Code: %d, Response Text: %s",
                    method,
                    url,
                    status_code,
                    respose_text,
                )
                return response.json()
