# source
source /Users/mshadish/.bash_profile

# specify the filename
filename="/Users/mshadish/cef_model/scrape_logs/log_`date`.log"

# write to the log file
python /Users/mshadish/cef_model/morningstar_scrape.py > "$filename"


# transform
python /Users/mshadish/cef_model/transformation.py | python /users/mshadish/cef_model/send_text.py