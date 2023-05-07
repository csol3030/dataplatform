apt update
tee /etc/apt/sources.list.d/notesalexp.list<<EOF
deb https://notesalexp.org/tesseract-ocr5/$(lsb_release -cs)/ $(lsb_release -cs) main
EOF
wget -O - https://notesalexp.org/debian/alexp_key.asc | apt-key add -
apt update
apt install tesseract-ocr -y