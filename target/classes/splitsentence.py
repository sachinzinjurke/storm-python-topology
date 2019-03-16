import storm

class SplitSentenceBolt(storm.BasicBolt):
    def process(self, tup):
        words = tup.values[0].split(" ")
        print("*********Python is fun.")
        for word in words:
          storm.emit([word])
SplitSentenceBolt().run()