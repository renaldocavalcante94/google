from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials


class GoogleAnalyticsAPI():

    scopes = ['https://www.googleapis.com/auth/analytics.readonly']

    def __init__(self,credential_path,view_id):
        self.key_location = credential_path
        self.view_id = str(view_id)
        self.analytics = self._initialize_analytics_reporting()

    def _initialize_analytics_reporting(self):
        credentials = ServiceAccountCredentials.from_json_keyfile_name(self.key_location, self.scopes)

        analytics = build('analyticsreporting', 'v4', credentials=credentials)

        return analytics

    def _build_request(self,startDate,endDate,metrics,dimensions,samplingLevel='LARGE',pageSize=1000,pageToken=None):
        
        if pageToken == None:
            response = self.analytics.reports().batchGet(
                body={
                    'reportRequests': [
                        {
                            'viewId': self.view_id,
                            "dateRanges": [{'startDate':startDate, 'endDate':endDate}],
                            'metrics': metrics,
                            'dimensions': dimensions,
                            'samplingLevel': samplingLevel,
                            'pageSize': pageSize
                        }
                    ]
                }
            ).execute()

        else: 
            response = self.analytics.reports().batchGet(
                body={
                    'reportRequests': [
                        {
                            'viewId': self.view_id,
                            "dateRanges": [{'startDate':startDate, 'endDate':endDate}],
                            'metrics': metrics,
                            'dimensions': dimensions,
                            'samplingLevel': samplingLevel,
                            'pageSize': pageSize,
                            'pageToken': pageToken
                        }
                    ]
                }
            ).execute()

        response = response["reports"][0]

        return response

    def _build_metrics_object(self,metrics_names):

        if type(metrics_names) != list:
            raise TypeError("metrics_names must be a  list of names of metrics")

        metric_frame = []

        for metric in metrics_names:
            x = {'expression': metric}

            metric_frame.append(x)

        return metric_frame

    def _build_dimensions(self,dimensions_names):
        if type(dimensions_names) != list:
            raise TypeError("dimensions_names must be a list of names of dimensions")

        dimensions_frame = []

        for dimension in dimensions_names:
            x = {"name": dimension}

            dimensions_frame.append(x)

        return dimensions_frame


class GoogleAnalyticsReportColumns():

    def __init__(self,columns_header):
        self.columns_header = columns_header
        self._dimensions_columns_builder()
        self._metrics_columns_builder()
        self._columns_builder()


    def _dimensions_columns_builder(self):        
        self.dimension_columns = self.columns_header["dimensions"]

    def _metrics_columns_builder(self):
        metric_columns_headers = self.columns_header["metricHeader"]["metricHeaderEntries"]

        metric_columns = []

        for x in metric_columns_headers:
            metric_columns.append(x["name"])

        self.metric_columns = metric_columns

    def _columns_builder(self):
        self.columns_names = self.dimension_columns + self.metric_columns


class GoogleAnalyticsReport(GoogleAnalyticsAPI):


    def __init__(self, credential_path, view_id):
        super().__init__(credential_path, view_id)
        self.startDate = None
        self.endDate = None
        self.pageSize = 10000

    def build_dimensions(self,dimensions_list):
        self.dimensions = self._build_dimensions(dimensions_list)

    def build_metrics(self,metrics_list):
        self.metrics = self._build_metrics_object(metrics_list)

    def build_columns(self):
        try:
            self.dimensions
            self.metrics
        except AttributeError:
            raise AttributeError("Dimensions and Metrics attribute must be builded before build the columns")
        
        response = self._build_request(self.startDate,self.endDate,self.metrics,self.dimensions,pageSize=1)
        columns_header = response["columnHeader"]

        ga_columns = GoogleAnalyticsReportColumns(columns_header)

        self.columns = ga_columns

    def get_sample(self,sample_size):
        if sample_size > 500:
            raise ValueError("sample_size must be lower than 500")

        response = self._build_request(self.startDate,self.endDate,self.metrics,self.dimensions,pageSize=sample_size)
        sample = response["data"]
        sample = self._process_data(sample)

        return sample


    def build_report(self):
        response  = self._build_request(self.startDate,self.endDate,self.metrics,self.dimensions,pageSize=self.pageSize)
        request_counter = 1
        print(f"Request {request_counter}")

        data = response["data"]
        data = self._process_data(data)

        if 'nextPageToken' in response.keys():
            pageToken = response["nextPageToken"]
        else:
            pageToken = None

        while pageToken != None:
            i_response = self._build_request(self.startDate,self.endDate,self.metrics,self.dimensions,pageSize=self.pageSize,pageToken=pageToken)
            request_counter += 1 
            
            print(f"Page Token {pageToken}")
            print(f"Request {request_counter}")

            i_data = i_response["data"]

            i_data = self._process_data(i_data)

            if 'nextPageToken' in i_response.keys():
                pageToken = i_response["nextPageToken"]
            else:
                pageToken = None

            data.append(i_data)

        self.data = data

    def _process_data(self,data):
        columns = self.columns
        
        rows = data["rows"]

        rows_values = []

        for row in rows:
            frame = {}

            for dimension,row_dimension_value in zip(columns.dimension_columns,row["dimensions"]):
                frame[dimension] = row_dimension_value

            row_metrics_value = row["metrics"][0]["values"]

            for metric,row_metric_value in zip(columns.metric_columns,row_metrics_value):
                frame[metric] = row_metric_value

            rows_values.append(frame)

        return rows_values

