from .api_handler import UrlBuilder
import argparse

def main():
    parser = argparse.ArgumentParser(description="F1 Stats")
    parser.add_argument("--year", type=int, default=2023)
    parser.add_argument("--session", type=str, default=None)
    parser.add_argument("--file", type=str, default=None)
    parser.add_argument("--meeting", type=str, default=None)
    args = parser.parse_args()
    
    session = UrlBuilder().with_year(args.year).with_session(args.session).with_file(args.file).with_meeting(args.meeting).execute()
    print(session)


if __name__ == "__main__":
    main()
