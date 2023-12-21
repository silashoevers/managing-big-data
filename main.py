import load_twitter
import filter
import process
import visualise

#source files to use
PATHS = ""

#load the paths
df_loaded = load_twitter.main(PATHS)

#filter users we need
df_filtered = filter.main(df_loaded)

#process the filtered data
df_processed = process.main(df_filtered)

#visualise results
visualise.main(df_processed)