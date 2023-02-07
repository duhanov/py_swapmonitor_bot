read -p "Enter Start Block Number : " start_block
read -p "Enter End Block Number : " end_block
#ssh -p 2200 root@ip <<-EOF
#        cd /home/maximus/www/krama24.by/current/public/system
#        tar -cvzf  brick_statics.tgz brick_statics
#    exit
#EOF
python3 app_parser.py --start_block=$start_block --end_block=$end_block --cmd=parse
