@echo off
echo Running read_flow.py with stdin from config_stream.txt...
type config_stream.txt | python read_flow.py
echo Done!
