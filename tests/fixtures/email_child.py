"""A real task process with a fixed SMTP EHLO name, independent of the runner's DNS."""

import socket

from etl_craft.execution import child

if __name__ == "__main__":
    socket.getfqdn = lambda name="": "smtp-client.test"
    child.run_as_process()
