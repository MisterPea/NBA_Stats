"""
When were importing some packages they weren't being
seen by the component/class/file. This script allows the file
to be seen as a module.
"""
import os
import sys

# Add the parent dir to sys.path
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, parent_dir)
