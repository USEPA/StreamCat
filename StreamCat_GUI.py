from database import DatabaseConnection
import customtkinter as ctk
ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")


class ProgressbarFrame(ctk.CTkToplevel):
    def __init__(self, parent):
        super().__init__(parent)
        self.parent = parent

class DbResultsFrame(ctk.CTkToplevel):
    def __init__(self, parent, response):
        super().__init__(parent)
        self.parent = parent
        self.geometry("400x300")

        self.label = ctk.CTkLabel(self, text="Database Response")
        self.label.pack(side=ctk.TOP, padx=20, pady=20)
        self.response = response
        self.response_str = ''
        if isinstance(self.response, (tuple, list)):
            for i in self.response:
                self.response_str += str(i)
        else:
            self.response_str = str(self.response)
        self.textbox = ctk.CTkTextbox(self)
        self.textbox.pack(expand=True, fill=ctk.BOTH)
        self.textbox.insert("0.0", text=self.response_str)

        self.textbox.configure(state="disabled")
    

class CreateDatasetFrame(ctk.CTkFrame):
    def __init__(self, parent):
        super().__init__(parent)
        self.parent = parent

        # Create widgets for creating a dataset
        self.partition_label = ctk.CTkLabel(self, text="What partition would you like to use?")
        self.partition_label.pack(padx=10, pady=10)

        self.partition_var = ctk.StringVar()
        self.partition_var.set('streamcat')
        self.partition_radio_streamcat = ctk.CTkRadioButton(self, text="StreamCat", variable=self.partition_var, value='streamcat')
        self.partition_radio_streamcat.pack(padx=10, pady=5)
        self.partition_radio_lakecat = ctk.CTkRadioButton(self, text="LakeCat", variable=self.partition_var, value='lakecat')
        self.partition_radio_lakecat.pack(padx=10, pady=5)

        self.files_label = ctk.CTkLabel(self, text="Choose files:")
        self.files_label.pack(fill=ctk.X, padx=10, pady=10)

        self.files_button = ctk.CTkButton(self, text="Browse", command=self.browse_files)
        self.files_button.pack(fill=ctk.X, padx=10, pady=5)

        self.files_entry = ctk.CTkEntry(self)
        self.files_entry.pack(fill=ctk.X, padx=10, pady=5)

        self.submit_button = ctk.CTkButton(self, text="Submit", command=self.create_dataset)
        self.submit_button.pack(fill=ctk.X, padx=10, pady=5)
        self.results_window = None
        
    def browse_files(self):
        files = ctk.filedialog.askopenfilenames()
        self.files_entry.delete(0, ctk.END)
        self.files_entry.insert(0, ', '.join(files))
    
    def create_dataset(self):
        #progressbar = ProgressbarFrame(self)
        progressbar_frame = ctk.CTkFrame(self)
        progressbar_frame.pack(side=ctk.BOTTOM)
        progressbar_frame.grid_columnconfigure(0, weight=1)
        progressbar_frame.grid_rowconfigure(1, weight=1)
        progressbar = ctk.CTkProgressBar(progressbar_frame, orientation='horizontal', mode='determinate')
        progressbar.configure(mode='determinate')
        
        partition = self.selected_partition.get().lower()
        files = self.files_entry.get()
        progressbar.start()
        ds_result, metric_result, display_result = db_conn.CreateDatasetFromFiles(partition, files)
        progressbar.stop()
        print(ds_result)
        print(metric_result)
        print(display_result)
        results = (ds_result, metric_result, display_result)
        self.results_window = DbResultsFrame(self, results)
        self.results_window.focus()



class CreateTableFrame(ctk.CTkFrame):
    def __init__(self, parent):
        super().__init__(parent)
        self.parent = parent

        # Create widgets for creating a table
        self.table_name_label = ctk.CTkLabel(self, text="Enter table name:")
        self.table_name_label.pack(fill=ctk.X, padx=10, pady=10)

        self.table_name_entry = ctk.CTkEntry(self)
        self.table_name_entry.pack(fill=ctk.X, padx=10, pady=5)

        files_btn = ctk.CTkButton(self, text="Choose Files", command=self.browse_files)
        files_btn.pack(fill=ctk.X, padx=10, pady=5)

        self.files_entry = ctk.CTkEntry(self)
        #self.files_entry.configure()
        self.files_entry.pack(fill=ctk.X, padx=10, pady=5)

        self.submit_button = ctk.CTkButton(self, text="Submit", command=self.create_table)
        self.submit_button.pack(fill=ctk.X, padx=10, pady=5)
        self.results_window = None

    def browse_files(self):
        files = ctk.filedialog.askopenfilenames()
        self.files_entry.delete(0, ctk.END)
        self.files_entry.insert(0, ', '.join(files))
    
    def create_table(self):
        table_name = self.table_name_entry.get()
        files = self.files_entry.get()
        results = db_conn.CreateTableFromFile(table_name, files)
        self.results_window = DbResultsFrame(self, results)
        self.results_window.focus()

class RenameStreamCatMetricFrame(ctk.CTkFrame):
    def __init__(self, parent):
        super().__init__(parent)
        self.parent = parent
        #self.grid_configure(column=3, row=3)

        # select all metrics from sc_metrics
        self.metric_name_results = db_conn.SelectColsFromTable('metricname', 'sc_metrics')
        self.metric_name_options = []
        for row in self.metric_name_results:
            self.metric_name_options.append(row._t[0])
        self.metric_name_var = ctk.StringVar()
        self.metric_name_var.set(self.metric_name_options[0])

        self.old_metric_label = ctk.CTkLabel(self, text="Select the metric you want to rename")
        self.old_metric_label.grid(row=1, column=0, padx=10, pady=5)
        

        self.metric_name_dropdown = ctk.CTkComboBox(self, variable=self.metric_name_var, values=self.metric_name_options)
        self.metric_name_dropdown.grid(row=1, column=1, padx=10, pady=5)

        self.new_metric_label = ctk.CTkLabel(self, text="Enter new metric name")
        self.new_metric_label.grid(row=2, column=0, padx=10, pady=5)
        self.new_name = ctk.CTkEntry(self)
        self.new_name.grid(row=2, column=1, padx=10, pady=5)

        self.submit_button = ctk.CTkButton(self, text="Submit", command=self.rename_metric)
        #self.submit_button.pack(side=ctk.BOTTOM, fill=ctk.X, padx=10, pady=5)
        self.submit_button.grid(row=3, column=0, columnspan=3, padx=10, pady=5)

        self.results_window = None
    
    def rename_metric(self):
        old_name = self.metric_name_var.get()
        new_name = self.new_name.get()
        results = db_conn.UpdateMetricName(old_name, new_name)
        self.results_window = DbResultsFrame(self, results)
        self.results_window.focus()

class ActivateDatasetFrame(ctk.CTkFrame):
    def __init__(self, parent):
        super().__init__(parent)
        self.parent = parent

        self.dsname_var = ctk.StringVar()
        self.dsname_options = db_conn.GetAllDatasetNames()
        self.dsname_var.set(self.dsname_options[0])
        self.dsname_dropdown = ctk.CTkComboBox(self, variable=self.dsname_var, values=self.dsname_options)
        self.dsname_dropdown.pack(fill=ctk.X, expand=True, padx=10, pady=5)
        self.submit_button = ctk.CTkButton(self, text="Submit", command=self.update_active_dataset)
        self.submit_button.pack(side=ctk.BOTTOM, fill=ctk.X, padx=10, pady=5)

        self.results_window = None

    def update_active_dataset(self):
        dsname = self.dsname_var.get()
        results = db_conn.UpdateActiveDataset(dsname)

        self.results_window = DbResultsFrame(self, results)
        self.results_window.focus()

class UpdateTableFrame(ctk.CTkFrame):
    def __init__(self, parent):
        super().__init__(parent)
        self.parent = parent

        self.table_dropdown_label = ctk.CTkLabel(self, text="Select table to update:")
        self.table_dropdown_label.pack(padx=10, pady=5)

        self.table_var = ctk.StringVar()
        self.table_options = list(db_conn.metadata.tables.keys())
        self.table_var.set(self.table_options[0])
        self.table_dropdown = ctk.CTkComboBox(self, variable=self.table_var, values=self.table_options)
        self.table_dropdown.pack(padx=10, pady=5)

        self.files_label = ctk.CTkLabel(self, text="Choose files:")
        self.files_label.pack(padx=10, pady=10)

        self.files_button = ctk.CTkButton(self, text="Browse", command=self.browse_files)
        self.files_button.pack(padx=10, pady=5)

        self.files_entry = ctk.CTkEntry(self)
        self.files_entry.pack(padx=10, pady=5)

        self.submit_button = ctk.CTkButton(self, text="Submit", command=self.update_table)
        self.submit_button.pack(side=ctk.BOTTOM, fill=ctk.X, padx=10, pady=5)

        self.results_window = None

    def browse_files(self):
        files = ctk.filedialog.askopenfilenames()
        self.files_entry.delete(0, ctk.END)
        self.files_entry.insert(0, ', '.join(files))

    def update_table(self):
        table_name = self.table_var.get()
        file = self.files_entry.get()
        results = db_conn.BulkInsertFromFile(table_name, file)

        self.results_window = DbResultsFrame(self, results)
        self.results_window.focus()

class CreateMetricInfoFrame(ctk.CTkFrame):
    def __init__(self, parent):
        super().__init__(parent)
        self.parent = parent

        self.metric_name_label = ctk.CTkLabel(self, text="Enter new metric name:")
        self.metric_name_label.grid(row=2, column=0, padx=10, pady=5)

        self.metric_name_entry = ctk.CTkEntry(self, width=280)
        self.metric_name_entry.grid(row=2, column=1, columnspan=2, padx=10, pady=5)
    
        self.category_label = ctk.CTkLabel(self, text="Select metric category:")
        self.category_label.grid(row=3, column=0, padx=10, pady=5)

        self.category_dropdown_var = ctk.StringVar()
        self.category_dropdown = ctk.CTkComboBox(self, width=280, variable=self.category_dropdown_var, values=["Base", "Natural", "Anthropogenic"])
        self.category_dropdown.grid(row=3, column=1, columnspan=2, padx=10, pady=5)
        

        self.aoi_label = ctk.CTkLabel(self, text="Select all AOIs for the metric:")
        self.aoi_label.grid(row=4, column=0, padx=10, pady=5)
        
        self.aoi_dropdown_var = ctk.StringVar()
        self.aoi_dropdown = ctk.CTkComboBox(self, width=280, variable=self.aoi_dropdown_var, values=["Cat", "Ws", "CatRp100", "WsRp100", "Other"])
        self.aoi_dropdown.grid(row=4, column=1, columnspan=2, padx=10, pady=5)
        
        self.year_label = ctk.CTkLabel(self, text="Enter comma seperated list of years (if available):")
        self.year_label.grid(row=5, column=0, padx=10, pady=5)
        
        self.year_entry = ctk.CTkEntry(self, width=280)
        self.year_entry.grid(row=5, column=1, columnspan=2, padx=10, pady=5)

        self.webtool_label = ctk.CTkLabel(self, text="Enter Webtool Name:")
        self.webtool_label.grid(row=6, column=0, padx=10, pady=5)
        self.webtool_entry = ctk.CTkEntry(self, width=280)
        self.webtool_entry.grid(row=6, column=1, columnspan=2, padx=10, pady=5)

        self.description_label = ctk.CTkLabel(self, text="Enter metric description:")
        self.description_label.grid(row=7, column=0, padx=10, pady=5)
        self.description_entry = ctk.CTkEntry(self, width=280)
        self.description_entry.grid(row=7, column=1, columnspan=2, padx=10, pady=5)

        self.units_label = ctk.CTkLabel(self, text="Enter metric units:")
        self.units_label.grid(row=8, column=0, padx=10, pady=5)
        self.units_entry = ctk.CTkEntry(self, width=280)
        self.units_entry.grid(row=8, column=1, columnspan=2, padx=10, pady=5)

        self.uuid_label = ctk.CTkLabel(self, text="Enter metric uuid:")
        self.uuid_label.grid(row=9, column=0, padx=10, pady=5)
        self.uuid_entry = ctk.CTkEntry(self, width=280)
        self.uuid_entry.grid(row=9, column=1, columnspan=2, padx=10, pady=5)

        self.metadata_label = ctk.CTkLabel(self, text="Enter metric metadata:")
        self.metadata_label.grid(row=10, column=0, padx=10, pady=5)
        self.metadata_entry = ctk.CTkEntry(self, width=280)
        self.metadata_entry.grid(row=10, column=1, columnspan=2, padx=10, pady=5)

        self.source_name_label = ctk.CTkLabel(self, text="Enter metric source name:")
        self.source_name_label.grid(row=11, column=0, padx=10, pady=5)
        self.source_name_entry = ctk.CTkEntry(self, width=280)
        self.source_name_entry.grid(row=11, column=1, columnspan=2, padx=10, pady=5)

        self.source_url_label = ctk.CTkLabel(self, text="Enter metric source url:")
        self.source_url_label.grid(row=12, column=0, padx=10, pady=5)
        self.source_url_entry = ctk.CTkEntry(self, width=280)
        self.source_url_entry.grid(row=12, column=1, columnspan=2, padx=10, pady=5)

        self.date_label = ctk.CTkLabel(self, text="Enter the date the dataset was downloaded with the format dd-MM-YY:")
        self.date_label.grid(row=13, column=0, padx=10, pady=5)
        self.date_entry = ctk.CTkEntry(self, width=280)
        self.date_entry.grid(row=13, column=1, columnspan=2, padx=10, pady=5)

        self.dsid_label = ctk.CTkLabel(self, text="Enter metric dsid:")
        self.dsid_label.grid(row=14, column=0, padx=10, pady=5)
        self.dsid_entry = ctk.CTkEntry(self, width=280)
        self.dsid_entry.grid(row=14, column=1, columnspan=2, padx=10, pady=5)

        self.dataset_name_label = ctk.CTkLabel(self, text="Enter metric dataset name:")
        self.dataset_name_label.grid(row=15, column=0, padx=10, pady=5)
         
        self.dataset_name_entry = ctk.CTkEntry(self, width=280)
        self.dataset_name_entry.grid(row=15, column=1, columnspan=2, padx=10, pady=5)

        self.partition_label = ctk.CTkLabel(self, text="What partition is the metric in?")
        self.partition_label.grid(row=16, column=0, padx=10, pady=10)
        
        self.selected_partition = ctk.StringVar(value='streamcat')
        self.streamcat_radio = ctk.CTkRadioButton(self, text="StreamCat", variable=self.selected_partition, value='streamcat')
        self.streamcat_radio.grid(row=16, column=1, sticky='w', padx=10, pady=5)
        self.lakecat_radio = ctk.CTkRadioButton(self, text="LakeCat", variable=self.selected_partition, value='lakecat')
        self.lakecat_radio.grid(row=16, column=2, sticky='w', padx=10, pady=5)

        self.submit_button = ctk.CTkButton(self, text="Submit", command=self.create_metric_info)
        self.submit_button.grid(row=17, column=0, columnspan=2, padx=10, pady=5)

        self.results_window = None

    def create_metric_info(self):
        print("Creating metric info card")
        metric_data = {}
        #metric_data['partition'] = self.selected_partition.get()
        table_name = 'sc_metrics_tg' if self.selected_partition.get() == 'streamcat' else 'lc_metrics_tg'
        metric_data['metric_name'] = self.metric_name_entry.get()
        metric_data['indicator_category'] = self.category_dropdown_var.get()
        metric_data['aoi'] = self.aoi_dropdown_var.get()
        metric_data['year'] = self.year_entry.get()
        metric_data['webtool_name'] = self.webtool_entry.get()
        metric_data['description'] = self.description_entry.get()
        metric_data['units'] = self.units_entry.get()
        metric_data['uuid'] = self.uuid_entry.get()
        metric_data['metadata'] = self.metadata_entry.get()
        metric_data['source_name'] = self.source_name_entry.get()
        metric_data['source_url'] = self.source_url_entry.get()
        metric_data['date_downloaded'] = self.date_entry.get()
        metric_data['dsid'] = self.dsid_entry.get()
        results = db_conn.InsertRow(table_name, metric_data)

        self.results_window = DbResultsFrame(self, results)
        self.results_window.focus()
        
class EditMetricInfoFrame(ctk.CTkFrame):
    def __init__(self, parent):
        super().__init__(parent)
        self.parent = parent
        self.partition_label = ctk.CTkLabel(self, text="What partition would you like to use?")
        self.partition_label.grid(row=1, column=0, padx=10, pady=5)

        self.partition_var = ctk.StringVar()
        self.partition_var.set('streamcat')
        self.partition_radio_streamcat = ctk.CTkRadioButton(self, text="StreamCat", variable=self.partition_var, value='streamcat')
        self.partition_radio_streamcat.grid(row=1, column=1, padx=10, pady=5)
        self.partition_radio_lakecat = ctk.CTkRadioButton(self, text="LakeCat", variable=self.partition_var, value='lakecat')
        self.partition_radio_lakecat.grid(row=1, column=2, padx=10, pady=5)

        metric_name_options, tg_columns = self.get_edit_widget_data()

        self.metric_name_label = ctk.CTkLabel(self, text="What metric variable info do you want to edit?")
        self.metric_name_label.grid(row=2, column=0, padx=10, pady=5)

        self.metric_name_var = ctk.StringVar()
        self.metric_name_dropdown = ctk.CTkComboBox(self, width=280, variable=self.metric_name_var, values=metric_name_options)
        self.metric_name_dropdown.grid(row=2, column=1, padx=10, pady=5)

        self.tg_col_label = ctk.CTkLabel(self, text="Which value needs to be edited?")
        self.tg_col_label.grid(row=3, column=0, padx=10, pady=5)

        self.tg_col_var = ctk.StringVar()
        self.tg_col_dropdown = ctk.CTkComboBox(self, width=280, variable=self.tg_col_var, values=tg_columns)
        self.tg_col_dropdown.grid(row=3, column=1, padx=10, pady=5)

        self.new_val_label = ctk.CTkLabel(self, text="Enter new value for selected metric value: ")
        self.new_val_label.grid(row=4, column=0, padx=10, pady=5)

        self.new_val_entry = ctk.CTkEntry(self, width=280)
        self.new_val_entry.grid(row=4, column=1, padx=10, pady=5)

        self.submit_button = ctk.CTkButton(self, text="Submit", command=self.edit_metric_info)
        self.submit_button.grid(row=5, column=0, padx=10, pady=5)
        
        self.results_window = None

    def get_edit_widget_data(self):
        table_name = 'sc_metrics_tg' if self.partition_var.get() == 'streamcat' else 'lc_metrics_tg'
        
        metric_name_results = db_conn.SelectColsFromTable('metric_name', table_name)
        metric_name_options = []
        for row in metric_name_results:
            metric_name_options.append(row._t[0])
        
        tg_columns = list(db_conn.metadata.tables[table_name].c.keys())

        return metric_name_options, tg_columns

    def edit_metric_info(self):
        table_name = 'sc_metrics_tg' if self.partition_var.get() == 'streamcat' else 'lc_metrics_tg'
        col_name = self.tg_col_var.get()
        metric_name = self.metric_name_var.get()
        new_val = self.new_val_entry.get()
        results = db_conn.UpdateRow(table_name, col_name, metric_name, new_val)
        self.results_window = DbResultsFrame(self, results)
        self.results_window.focus()


class DatabaseApp(ctk.CTk):
    def __init__(self):
        super().__init__()
        #self = root

        self.title("StreamCat GUI (modern)")
        # self.geometry("700x450")
        self.width = int(self.winfo_screenwidth()/2)
        self.height = int(self.winfo_screenheight()/1.5)
        self.geometry(f"{self.width}x{self.height}")
        self.minsize(500,500)
        self.action_frames = {}
        self.current_frame = None

        self.action_var = ctk.StringVar()
        # self.action_var.set('Create Dataset')

        self.actions = [
            'Create Dataset',
            'Create Table',
            'Rename Metric',
            'Activate/Deactivate Dataset',
            'Add File Data to Table',
            'Create Metric Info',
            'Edit Metric Info'
        ]
        self.action_var.set(self.actions[0]) # could add a default / info frame to be actions[0] called '--'
        self.action_dropdown = ctk.CTkComboBox(self, variable=self.action_var, values=self.actions)
        self.action_dropdown.pack(side=ctk.TOP, padx=10, pady=10)

        self.action_button = ctk.CTkButton(self, text="Go", command=self.show_frame)
        self.action_button.pack(side=ctk.TOP, padx=10, pady=5)

        self.show_frame()

    def show_frame(self):
        action = self.action_var.get()
        if action not in self.action_frames:
            if action == 'Create Dataset':
                self.action_frames[action] = CreateDatasetFrame(self)
            elif action == 'Create Table':
                self.action_frames[action] = CreateTableFrame(self)
            elif action == 'Rename Metric':
                self.action_frames[action] = RenameStreamCatMetricFrame(self)
            elif action == 'Activate/Deactivate Dataset':
                self.action_frames[action] = ActivateDatasetFrame(self)
            elif action == 'Add File Data to Table':
                self.action_frames[action] = UpdateTableFrame(self)
            elif action == 'Create Metric Info':
                self.action_frames[action] = CreateMetricInfoFrame(self)
            elif action == 'Edit Metric Info':
                self.action_frames[action] = EditMetricInfoFrame(self)
            

        if self.current_frame:
            self.current_frame.pack_forget()

        self.current_frame = self.action_frames[action]
        self.current_frame.pack(fill='both', expand=True)

if __name__ == '__main__':
    db_conn = DatabaseConnection()
    db_conn.connect()
    app = DatabaseApp()
    app.mainloop()