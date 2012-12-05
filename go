hadoop jar 18645-proj4-0.1-latest.jar -program catrank -input data/ -output results -tmpdir tmp

# start emr
elastic-mapreduce --create --num-instances 5 --instance-type c1.medium

# end emr
elastic-mapreduce --terminate <$JID>

# catrank on EMR
elastic-mapreduce --jobflow $JID --jar s3n://wenjunzh.src/proj4-v2.jar --arg -program --arg catrank --arg -input --arg s3n://wenjunzh.wiki4g/ --arg -output --arg s3n://wenjunzh.output/wiki4gv2 --arg -tmpdir --arg tmp

