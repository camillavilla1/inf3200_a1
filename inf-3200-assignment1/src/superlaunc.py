import argparse
import launch
import subprocess

subprocess.call('./launch.py --nameserver compute-3-16:8088 compute-3-17:8010 --run-tests')
