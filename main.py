import argparse

from iota import iota_api


def command_line_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--serve-global',
        action='store_true',
        help='serve globally'
    )
    parser.add_argument(
        '--port', '-p', 
        type=int, 
        default=8000, 
        help='port to serve'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='enable debug mode'
    )
    args = parser.parse_args()
    args.host = '0.0.0.0' if args.serve_global else None
    if not args.debug:
        args.debug = None

    return args


def main():
    args = command_line_args()
    app = iota_api.create_app()
    app.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == '__main__':
    main()
