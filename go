# ngramcount tweets10m
hadoop jar 18645-proj3-0.1-latest.jar -program ngramcount -input data/tweets10m -output data/ngram10m

# hashtagsim tweets1m
hadoop jar 18645-proj3-0.1-latest.jar -program hashtagsim -input data/tweets1m -output data/hashtag1m -tmpdir tmp

# ngramcount on EMR
elastic-mapreduce --jobflow $JID --jar s3n://18645.wenjunzh/18645-proj3-0.1-latest.jar --arg -program --arg ngramcount --arg -input --arg s3n://wenjunzh.tweets10m/ --arg -output --arg s3n://wenjunzh.output/ngram10m



