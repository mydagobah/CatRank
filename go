hadoop jar 18645-proj4-0.1-latest.jar -program catrank -input data/ -output results -tmpdir tmp


# ngramcount on EMR
elastic-mapreduce --jobflow $JID --jar s3n://18645.wenjunzh/18645-proj4-0.1-latest.jar --arg -program --arg catrank --arg -input --arg s3n://wenjunzh.tweets10m/ --arg -output --arg s3n://wenjunzh.output/ngram10m

