import pandas as pd
import io
import json
import requests

COLNUMS = 8

def validate_cols(_cols):
  for c in _cols:
    assert c in (["col"+str(i+1) for i in range(COLNUMS)] + ["labels"])

def validate_funcs(_func):
  for f in _func:
    print(f)
    assert f[0] in ["AVG", "VAR", "COUNT"]
    assert f[1] in (["col"+str(i+1) for i in range(COLNUMS)] + ["labels"])

def validate_eps(_eps):
  assert _eps > 0
  assert _eps <= 20

def validate_sd_method(_method):
  assert _method in ["MWEM", "MST", "DPCTGAN", "PATECTGAN"]

def sql_params(cols, func, eps):
  funcs = ', '.join([f"{f[0]}({f[1]}) AS labels_{f[0]}" for f in func])
  cols = ', '.join(cols)
  return {
      "eps": eps,
      "query_str": f"SELECT {cols}, {funcs} FROM comp.comp GROUP BY {cols}"
  }
    
def generate_sql_query(url, cols, func, eps) -> dict:
  validate_cols(cols)
  validate_funcs(func)
  validate_eps(eps)
  out = {
      "url": url+'query',
      "params": sql_params(cols, func, eps)
  }
  return out


def parse_synth_data_result(result) -> pd.DataFrame:
    if result.status_code != 200:
        print(f"Request recieved an error code: {result.status_code}")
        return None
    data = result.content.decode('utf8')
    df = pd.read_csv(io.StringIO(data))
    return df 
    
def parse_sql_query_result(response) -> pd.DataFrame:
  if response.status_code != 200:
    print(f"Request recieved an error code: {response.status_code}")
    return None
  res = json.loads(response.content.decode('utf-8'))
  return pd.DataFrame(res[1:], columns=res[0])


def generate_synth_data(url, synth_model, eps) -> dict:
  validate_sd_method(synth_model)
  validate_eps(eps)
  out = {
      "url": url+'synthesize',
      "params": {
          "model": synth_model,
          "eps": eps
      }
  }
  return out

def parse_synth_data_result(result) -> pd.DataFrame:
    if result.status_code != 200:
        print(f"Request recieved an error code: {result.status_code}")
        return None
    data = result.content.decode('utf8')
    df = pd.read_csv(io.StringIO(data))
    return df 


def run_query(url, cols, func, eps):

	query_dict = generate_sql_query(url, cols, func, eps)

	response = requests.get(**query_dict)

	result_df = parse_sql_query_result(response)

	return result_df


def get_synth_data(url, synthn_model, eps):

	raw_data = requests.get(**generate_synth_data(url, synth_model, eps))

	result_df = parse_synth_data_result(raw_data)

	return result_df








# function to submit an array or list of predictions for comp

def submit_predictions_comp(comp_url, user_name, predictions):

  test_x = pd.read_csv('./AIEthicon/data/TEST_X.csv',index_col='id')

  # make prediciton dataframe and save to file

  pred_df = pd.DataFrame(data=predictions, columns = ['labels'],index = test_x.index)

  pred_df.to_csv('submission.csv')

  response = requests.post(comp_url+'/submit', files = {"file": open("submission.csv", "rb")}, headers={"X-OBLV-User-Name":user_name})

  return response

# function to submit an array or list of predictions for sandbox

def submit_predictions_comp(comp_url, user_name, predictions):

  test_x = pd.read_csv('./AIEthicon/data/SANDBOX_TEST_X.csv',index_col='id')

  # make prediciton dataframe and save to file

  pred_df = pd.DataFrame(data=predictions, columns = ['labels'],index = test_x.index)

  pred_df.to_csv('submission.csv')

  response = requests.post(comp_url+'/submit', files = {"file": open("submission.csv", "rb")}, headers={"X-OBLV-User-Name":user_name})

  return response
