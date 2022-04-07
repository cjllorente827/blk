import datetime, subprocess

def noop():
    pass

def format_time(seconds):
    return str(datetime.timedelta(seconds=seconds))

def movie(movie_filename, plot_file_format):
    print("Running conversion to mp4 format...")
    cmd = f"ffmpeg -y -start_number 0 -framerate 10 -i {plot_file_format} -s 1440x1080 -vcodec libx264 -pix_fmt yuv420p {movie_filename}"
    subprocess.run(cmd, shell=True)