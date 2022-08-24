import datetime, subprocess

def do_nothing(*args, **kwargs):
    pass

def format_time(seconds):
    return str(datetime.timedelta(seconds=seconds))

def movie(movie_filename, plot_file_format):
    print("Running conversion to mp4 format...")
    cmd = f"sbatch --export=FILE_FORMAT={plot_file_format},MOVIE_NAME={movie_filename} ~/run_ffmpeg.sb"
    subprocess.run(cmd, shell=True)