from rake_nltk.rake import Rake
import boto3
import time
# sudo pip3 install python-rake
# pip install boot3
class Algorithms:
    boto3.set_stream_logger('botocore', level='DEBUG')
    def initialize(self, title, description, uniqueId):
        jsonResults = {}
        try:
            key = uniqueId
            data = self.process_algorithm(title,0000000, uniqueId, 1,'T')
            data.extend(self.process_algorithm(description,0000000, uniqueId, 1,'D'));
            #text = text.translate(str.maketrans('', '', string.punctuation))
            #text = re.sub(r'[^\w]', ' ', text)
            jsonResults[key] = data
            #summary_text = summary_text + ' ' + text
        except Exception as exc:
            print('%r generated an exception: %s' % (key, exc))
        return jsonResults

    def process_algorithm(self, text, id, uniqueId, flag, type):
        r = Rake()
        r.extract_keywords_from_text(text)
        score = r.get_ranked_phrases_with_scores()
        frequency = r.get_word_frequency_distribution()
        data = []
        data.extend(self.create_Key_Value(score, id, uniqueId, flag, type))
        data.extend(self.organize(dict(frequency), id, uniqueId, flag, type))
       # if(flag==1):
        data.extend(self.comprehend_algo(text, id, uniqueId, flag, type))
        return data

    def comprehend_algo(self, text, id, uniqueId, flag, type):
        #TODO keep it environment configuration files. Security Keys.
        comprehend = boto3.client(aws_access_key_id='AKIAJPS2BREVEGO3DBRA', aws_secret_access_key = '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',service_name='comprehend', region_name='us-east-1')
        time.sleep(1)
        index_keyword = dict(comprehend.detect_key_phrases(Text=text, LanguageCode='en'))
        return self.format_key_value(index_keyword['KeyPhrases'], id, uniqueId, flag, type)

    def format_key_value(self, d, id, uniqueId, flag, type):
        data_list = []
        d = sorted(d, key=lambda k: k['Score'], reverse=True)
        unit_max = None
        for data in d:
            if (not unit_max):
                unit_max = float(1 / data.get('Score'))
                print(unit_max)
            if (data.get('Score') * unit_max) > 0.9399:
                dictionary = {
                    'score': data.get('Score'),
                    'text': data.get('Text'),
                    'algorithmType': 'COMPREHEND',
                    'type': 'TITLE' if (type == 'T') else 'DESCRIPTION'
                }
                if (flag == 1):
                    dictionary['uniqueCrawledId'] = uniqueId
                    dictionary['summaryFlag'] = 1
                else:
                    dictionary['amazonDataId'] = id
                data_list.append(dictionary)
            else:
                break;
        return data_list

    def create_Key_Value(self, score_list, id, uniqueId, flag, type):
        data_list = []
        unit_max = None
        i = 0;
        for data in score_list:
            if unit_max is None:
                unit_max = float(1 / data[0])
                print(unit_max)
            if (unit_max * data[0]) > 0.8999 or i<5:
                dictionary = {
                    'text': data[1],
                    'score': data[0],
                    'algorithmType': 'RAKE',
                    'type': 'TITLE' if(type =='T') else 'DESCRIPTION'
                }
                if (flag == 1):
                    dictionary['uniqueCrawledId'] = uniqueId
                    dictionary['summaryFlag'] = 1
                else:
                    dictionary['amazonDataId'] = id
                data_list.append(dictionary)
                i=i+1
            else:
                break;
        return data_list

    def organize(self, da, id, uniqueId, flag,type):
        d = sorted(da.items(), key=lambda pair: pair[1], reverse=True)
        data_list = []
        endloop= 19 if(flag==1) else 9
        i = 0
        for data in d:
            if self.is_number(data[0]):
                continue;
            if (int(data[1]) < 3 or i > endloop):
                break
            else:
                dictionary = {
                    'text': data[0],
                    'score': data[1],
                    'algorithmType': 'WORDCOUNT',
                    'type': 'TITLE' if (type == 'T') else 'DESCRIPTION'
                }
                if (flag == 1):
                    dictionary['uniqueCrawledId'] = uniqueId
                    dictionary['summaryFlag'] = 1
                else:
                    dictionary['amazonDataId'] = id
                i = i + 1
                data_list.append(dictionary)
        return data_list

    def is_number(self,n):
        is_number = True
        try:
            num = float(n)
            # check for "nan" floats
            is_number = num == num  # or use `math.isnan(num)`
        except ValueError:
            is_number = False
        return is_number