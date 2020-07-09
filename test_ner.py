import spacy
import PyPDF2
from spacy import displacy
import pandas as pd
import json
import boto3

files = ['Shakespeareforarticle.pdf','Short Biography William Shakespeare.pdf',
         'shakespeare_overview.pdf','william_shakespeare-1.pdf']


queue_url = 'https://sqs.ap-south-1.amazonaws.com/875177437853/test-1'

nlp = spacy.load('en_core_web_md')


def extract(nlp,files):

  def _joinTup(x):
    return x.text


  output = dict()
  for fileNo,file in enumerate(files):
    pdfFileObj = open(file, 'rb')
    pdfReader = PyPDF2.PdfFileReader(pdfFileObj)
    pages = pdfReader.numPages
    print(pages)

    for page in range(pages):
      pageObj = pdfReader.getPage(page)
      text = pageObj.extractText()

      doc = nlp(text)

      df = pd.DataFrame({'entities':list(map(_joinTup,list(doc.ents))),'label':[i.label_ for i in doc.ents],
                       'start_char':[[i.start_char] for i in doc.ents],
                       'end_char':[[i.end_char] for i in doc.ents]}).\
                       groupby(by = ['entities','label']).sum().reset_index()
      
      if df.empty == True:
        continue
      
      else:

        entities = list(map(lambda x,y: x+'---'+y,df['entities'].str.lower().values,df['label'].str.lower().values))

        for counter,entity in enumerate(entities):
          if entity in output.keys():
            output[entity][fileNo] = dict()
            output[entity][fileNo][page] = dict()
            output[entity][fileNo][page]['Start_char'] = df['start_char'].values[counter]
            output[entity][fileNo][page]['End_char'] = df['end_char'].values[counter]
          
          else:
            output[entity] = dict()
            output[entity][fileNo] = dict()
            output[entity][fileNo][page] = dict()
            output[entity][fileNo][page]['Start_char'] = df['start_char'].values[counter]
            output[entity][fileNo][page]['End_char'] = df['end_char'].values[counter]
  return output


def configureaws():
    client = boto3.client(
        'sqs',
        region_name='ap-south-1',
        aws_access_key_id='AKIA4XRFD5KOTQWJWBFU',
        aws_secret_access_key='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',
    )
    return client


def readmsg():
    # Create SQS client
    sqs = configureaws()

    queue_url = 'https://sqs.ap-south-1.amazonaws.com/875177437853/test-1'

    # Receive message from SQS queue
    response = sqs.receive_message(
        QueueUrl=queue_url,
       # MaxNumberOfMessages=1,
        VisibilityTimeout=0,
       # WaitTimeSeconds=0
    )
    # Delete received message from queue
    #sqs.delete_message(
    #    QueueUrl=queue_url,
    #    ReceiptHandle=receipt_handle
    #)

    #sqs.delete_message(QueueUrl=queue_url,ReceiptHandle=response['Messages'][0]['ReceiptHandle'])

    return response

def postmessage(body):

    sqs = configureaws()

    queue_url = 'https://sqs.ap-south-1.amazonaws.com/875177437853/test-1'

    # Send message to SQS queue
    response = sqs.send_message(
        QueueUrl=queue_url,
        DelaySeconds=10,
        MessageBody=body)
    return None


def main():
	sqs = configureaws()
	postmessage("trigger")

	check = True
	while check == True:
		message = readmsg()
		print(message)
		if 'Messages' in list(message.keys()):
			print(message.keys())
			sqs.delete_message(QueueUrl=queue_url,ReceiptHandle=message['Messages'][0]['ReceiptHandle'])
			output = extract(nlp,files)
			# print(output)

		else:
			check = False


if __name__ == '__main__':
    main()
