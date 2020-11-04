import requests
import pandas as pd
from copy import deepcopy
from datetime import datetime


# for final result 
result_df = []

today_date_in_str = datetime.now().strftime('%d-%m-%Y')
print(f'Downloading file for {today_date_in_str}')

# downloading new file
url = f'http://pakirsa.gov.pk/Doc/Data{today_date_in_str}.pdf'
r = requests.get(url, allow_redirects=True)
open(f'{today_date_in_str}.pdf', 'wb').write(r.content)

# reading pdf
text_pdf = convert_pdf_to_txt(f'{today_date_in_str}.pdf')

text_dict = {}
for index, line in enumerate(text_pdf.split('\n')):
    text_dict[index] = line

result = {}
result['INDUS @ TARBELA'] = {
    'LEVEL': text_dict[61],
    'DEAD LEVEL': text_dict[62],
    'MEAN INFLOW': text_dict[63],
    'MEAN OUTFLOW': text_dict[64]
}

result['KALABAGH'] = {
    'U/S DISCHARGE': text_dict[66],
    'D/S DISCHARGE': text_dict[67],
    'Thal': text_dict[68]
}

result['TAUNSA'] = {
    'U/S DISCHARGE': text_dict[70],
    'D/S DISCHARGE': text_dict[71],
    'T-P Link': text_dict[72],
    'Muzafarghar Canal': text_dict[73],
    'Dera Ghazi Khan Canal': text_dict[74]
}

result['SUKKUR'] = {
    'U/S DISCHARGE': text_dict[76],
    'D/S DISCHARGE': text_dict[77],
    '* Canal W/dls': text_dict[78]
}

result['JHELUM @ MANGLA'] = {
    'LEVEL': text_dict[80],
    'DEAD LEVEL': text_dict[81],
    'MEAN INFLOW': text_dict[82],
    'MEAN OUTFLOW': text_dict[83]
}

result['PANJNAD'] = {
    'U/S DISCHARGE': text_dict[92],
    'D/S DISCHARGE': text_dict[93],
}

result['KABUL @ NOWSHERA'] = {
    'MEAN DISCHARGE': text_dict[125]
}

result['CHASHMA'] = {
    'LEVEL': text_dict[183],
    'DEAD LEVEL': text_dict[184],
    'MEAN INFLOW': text_dict[185],
    'MEAN OUTFLOW': text_dict[186],
    'C-J Link': text_dict[187],
    'CRBC': text_dict[188]
}

result['GUDDU'] = {
    'U/S DISCHARGE': text_dict[190],
    'D/S DISCHARGE': text_dict[191],
    '* Canal W/dls': text_dict[192]
}

result['KOTRI'] = {
    'U/S DISCHARGE': text_dict[194],
    'D/S DISCHARGE': text_dict[195],
    'Canal W/dls': text_dict[196]
}

result['CHENAB @ MARALA'] = {
    'U/S DISCHARGE': text_dict[198],
    'D/S DISCHARGE': text_dict[199],
}

result['TOTAL RIM STATION'] = {
    'INFLOWS': text_dict[201],
    'OUTFLOWS': text_dict[203],
}

result['IRSA RELEASES'] = {
    'Punjab': text_dict[158],
    'Sindh': text_dict[159],
    'KPK': text_dict[228],
    'Balochistan': text_dict[229]
}

row_dict = []
for k1 in result.keys():
    for k2 in result[k1].keys():
        key = f'{k1}_{k2}'.replace(' ', '_')
        
        row_dict.append({
            'RIVER': k1,
            'SITUATION': k2,
            'VALUE': result[k1][k2],
            'DATE': today_date_in_str
        })

result_df += row_dict
df = pd.DataFrame(deepcopy(result_df))

df.DATE = df.DATE.apply(lambda x: fix_date(x))
df.VALUE = df.VALUE.apply(lambda x: fix_value(x))
old_df = pd.read_csv('pakirsa.csv').fillna('')
df = pd.concat([old_df,df])
df.DATE = df.DATE.apply(str).str[:10]
df.to_csv('pakirsa.csv', index=False)
