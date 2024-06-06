#!/bin/bash

# ��������� ������� ������ pipenv
if ! command -v pipenv &>/dev/null; then
    echo "������������� ����� pipenv"
    pip install pipenv
fi

# ��������� ����� pipenv
pipenv update

# ��������� � ����� �������
cd /myproject/windyapi/

# ��������� ������� ������������ ���������
if [ ! -d venv ]; then
    echo "������ ����������� ���������"
    python3 -m venv venv
fi

# ���������� ����������� ���������
. venv/bin/activate

# ��������� ����
python run_windy.py

# ������������ ������
if [ $? -ne 0 ]; then
    echo "������ ��� ������� run_windy.py"
    exit 1
fi