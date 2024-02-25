import signal, time, pandas as pd
from nse_announcements import NseFetch

nsefetch = NseFetch()

if __name__ == "__main__":
    signal.signal(signal.SIGINT, nsefetch.handle_stop_signals)
    signal.signal(signal.SIGTERM, nsefetch.handle_stop_signals)
    while True:
        try:
            df = nsefetch.get_corporate_announcement(index="equities")
            if df is not None and isinstance(df, pd.DataFrame):
                df.to_csv("equities_corporate_announcements")
            print(df)
        except KeyboardInterrupt:
            nsefetch.handle_stop_signals
            break
        else:
            pass
        time.sleep(3)
