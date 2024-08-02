from database import DatabaseConnection
import tkinter as tk
from tkinter import ttk, filedialog, simpledialog
from tkinter.messagebox import showinfo
from enum import Enum

# Could be useful to expand this oop idea
# class Options(Enum):
#     CREATE_DS = 'Create Dataset from files'
#     RENAME = 'Rename Metrics'
#     ACTIVATE = 'Activate/Deactivate Dataset'


def on_action_select(*args):
    """On click for action dropdown clear widget area and display correct widgets for selected action
    choice: dropdown option selected
    """
    choice = dropdown_var.get()
    if len(additional_widgets) > 0:
        #print(additional_widgets)
        for widget in additional_widgets:
            #print(widget)
            #print(type(widget))
            widget.grid_forget()
    additional_widgets.clear()
    
    
    if choice == 'Create Dataset from files':
        
        partition_label = ttk.Label(root, text="What partition would you like to use?")
        partition_label.grid(row=2, column=1, padx=10, pady=10)
        additional_widgets.insert(0, partition_label)

        streamcat_radio = ttk.Radiobutton(root, text="StreamCat", variable=selected_partition, value='streamcat')
        streamcat_radio.grid(row=3, column=1, sticky='w', padx=10, pady=5)
        lakecat_radio = ttk.Radiobutton(root, text="LakeCat", variable=selected_partition, value='lakecat')
        lakecat_radio.grid(row=3, column=2, sticky='w', padx=10, pady=5)
        additional_widgets.insert(1, streamcat_radio)
        additional_widgets.insert(2, lakecat_radio)

        files_btn = ttk.Button(root, text="Choose Files", command=lambda: choose_files(file_display))
        files_btn.grid(row=4, column=1, padx=10, pady=5)
        additional_widgets.insert(3, files_btn)

        file_display = ttk.Entry(root)
        file_display.grid(row=5, column=1, columnspan=3, padx=10, pady=5)
        additional_widgets.insert(4, file_display)

        #print(additional_widgets)

    elif choice == 'Create Table from Files':

        new_table_label = ttk.Label(root, text="New Table Name:")
        new_table_label.grid(row=2, column=0, padx=10, pady=5)
        additional_widgets.insert(0, new_table_label)

        entry_table_name = ttk.Entry(root)
        entry_table_name.grid(row=2, column=1, padx=10, pady=5)
        additional_widgets.insert(1, entry_table_name)

        files_btn = ttk.Button(root, text="Choose Files", command=lambda: choose_files(file_display))
        files_btn.grid(row=3, column=1, padx=10, pady=5)
        additional_widgets.insert(2, files_btn)

        file_display = ttk.Entry(root)
        file_display.grid(row=4, column=1, columnspan=2, padx=10, pady=5)
        additional_widgets.insert(3, file_display)

        #print(additional_widgets)
        
    elif choice == 'Rename Metric':
        old_name_label = ttk.Label(root, text="Old Metric Name:")
        old_name_label.grid(row=2, column=0, padx=10, pady=5)
        additional_widgets.insert(0, old_name_label)
        entry_old_name = ttk.Entry(root)
        entry_old_name.grid(row=2, column=1, padx=10, pady=5)
        additional_widgets.insert(1, entry_old_name)
        
        new_name_label = ttk.Label(root, text="New Metric Name:")
        new_name_label.grid(row=3, column=0, padx=10, pady=5)
        additional_widgets.insert(2, new_name_label)
        entry_new_name = ttk.Entry(root)
        entry_new_name.grid(row=3, column=1, padx=10, pady=5)
        additional_widgets.insert(3, entry_new_name)
        #print(additional_widgets)
        
    elif choice == 'Activate/Deactivate Dataset':
        dsname_label = ttk.Label(root, text="Dataset Name:")
        dsname_label.grid(row=2, column=0, padx=10, pady=5)
        additional_widgets.insert(0, dsname_label)
        entry_dataset_name = ttk.Entry(root)
        entry_dataset_name.grid(row=2, column=1, padx=10, pady=5)
        additional_widgets.insert(1, entry_dataset_name)
        #print(additional_widgets)
    
    elif choice == 'Update Table With File Data':
        table_choices_label = ttk.Label(root, text="Select table to update:")
        table_choices_label.grid(row=2, column=0, padx=10, pady=5)
        additional_widgets.insert(0, table_choices_label)

        #table_name_dropdown = ttk.Entry(root)
        
        table_name_dropdown = ttk.Combobox(root, textvariable=table_name_dropdown_var, values=table_options)
        #table_name_dropdown.bind("<<ComboboxSelected>>", on_action_select)
        table_name_dropdown.grid(row=3, column=1, columnspan=3, padx=10, pady=5)
        additional_widgets.insert(1, table_name_dropdown)

        files_btn = ttk.Button(root, text="Choose Files", command=lambda: choose_files(file_display))
        files_btn.grid(row=4, column=1, padx=10, pady=5)
        additional_widgets.insert(2, files_btn)

        file_display = ttk.Entry(root)
        file_display.grid(row=5, column=1, columnspan=2, padx=10, pady=5)
        additional_widgets.insert(3, file_display)
    
    elif choice =='Create Metric Info':
        metric_name_label = ttk.Label(root, text="Enter new metric name:")
        metric_name_label.grid(row=2, column=0, padx=10, pady=5)
        additional_widgets.append(metric_name_label) # 0
        metric_name_entry = ttk.Entry(root)
        metric_name_entry.grid(row=2, column=1, columnspan=2, padx=10, pady=5)
        additional_widgets.append(metric_name_label) #1

        category_label = ttk.Label(root, text="Select metric category:")
        category_label.grid(row=3, column=0, padx=10, pady=5)
        additional_widgets.append(category_label) #2
        category_dropdown = ttk.Combobox(root, textvariable=category_dropdown_var, values=categories)
        category_dropdown.grid(row=3, column=1, columnspan=2, padx=10, pady=5)
        additional_widgets.append(category_dropdown) #3

        aoi_label = ttk.Label(root, text="Select all AOIs for the metric:")
        aoi_label.grid(row=4, column=0, padx=10, pady=5)
        additional_widgets.append(aoi_label) #4
        aoi_dropdown = tk.Listbox(root, selectmode="multiple", listvariable=aoi_dropdown_var)
        for i, aoi in enumerate(aois):
            aoi_dropdown.insert(i, aoi)
        aoi_dropdown.grid(row=4, column=1, columnspan=2, padx=10, pady=5)
        additional_widgets.append(aoi_dropdown) #5

        year_label = ttk.Label(root, text="Enter comma seperated list of years (if available):")
        year_label.grid(row=5, column=0, padx=10, pady=5)
        additional_widgets.append(year_label) #6
        year_entry = ttk.Entry(root)
        year_entry.grid(row=5, column=1, columnspan=2, padx=10, pady=5)
        additional_widgets.append(year_entry) #7

        webtool_label = ttk.Label(root, text="Enter Webtool Name:")
        webtool_label.grid(row=6, column=0, padx=10, pady=5)
        additional_widgets.append(webtool_label) #8
        webtool_entry = ttk.Entry(root)
        webtool_entry.grid(row=6, column=1, columnspan=2, padx=10, pady=5)
        additional_widgets.append(webtool_entry) #9

        description_label = ttk.Label(root, text="Enter metric description:")
        description_label.grid(row=7, column=0, padx=10, pady=5)
        additional_widgets.append(description_label) #10
        description_entry = ttk.Entry(root)
        description_entry.grid(row=7, column=1, columnspan=2, padx=10, pady=5)
        additional_widgets.append(description_entry) #11

        units_label = ttk.Label(root, text="Enter metric units:")
        units_label.grid(row=8, column=0, padx=10, pady=5)
        additional_widgets.append(units_label) #12
        units_entry = ttk.Entry(root)
        units_entry.grid(row=8, column=1, columnspan=2, padx=10, pady=5)
        additional_widgets.append(units_entry) #13

        uuid_label = ttk.Label(root, text="Enter metric uuid:")
        uuid_label.grid(row=9, column=0, padx=10, pady=5)
        additional_widgets.append(uuid_label) #14
        uuid_entry = ttk.Entry(root)
        uuid_entry.grid(row=9, column=1, columnspan=2, padx=10, pady=5)
        additional_widgets.append(uuid_entry) #15

        metadata_label = ttk.Label(root, text="Enter metric metadata:")
        metadata_label.grid(row=10, column=0, padx=10, pady=5)
        additional_widgets.append(metadata_label) #16
        metadata_entry = ttk.Entry(root)
        metadata_entry.grid(row=10, column=1, columnspan=2, padx=10, pady=5)
        additional_widgets.append(metadata_entry) #17

        source_name_label = ttk.Label(root, text="Enter metric source name:")
        source_name_label.grid(row=11, column=0, padx=10, pady=5)
        additional_widgets.append(source_name_label) #18
        source_name_entry = ttk.Entry(root)
        source_name_entry.grid(row=11, column=1, columnspan=2, padx=10, pady=5)
        additional_widgets.append(source_name_entry) #19

        source_url_label = ttk.Label(root, text="Enter metric source url:")
        source_url_label.grid(row=12, column=0, padx=10, pady=5)
        additional_widgets.append(source_url_label) #20
        source_url_entry = ttk.Entry(root)
        source_url_entry.grid(row=12, column=1, columnspan=2, padx=10, pady=5)
        additional_widgets.append(source_url_entry) #21

        date_label = ttk.Label(root, text="Enter metric date with the format dd-MON-YY:")
        date_label.grid(row=13, column=0, padx=10, pady=5)
        additional_widgets.append(date_label) #22
        date_entry = ttk.Entry(root)
        date_entry.grid(row=13, column=1, columnspan=2, padx=10, pady=5)
        additional_widgets.append(date_entry) #23

        dsid_label = ttk.Label(root, text="Enter metric dsid:")
        dsid_label.grid(row=14, column=0, padx=10, pady=5)
        additional_widgets.append(dsid_label) #24
        dsid_entry = ttk.Entry(root)
        dsid_entry.grid(row=14, column=1, columnspan=2, padx=10, pady=5)
        additional_widgets.append(dsid_entry) #25

        dataset_name_label = ttk.Label(root, text="Enter metric dataset name:")
        dataset_name_label.grid(row=15, column=0, padx=10, pady=5)
        additional_widgets.append(dataset_name_label) #26
        dataset_name_entry = ttk.Entry(root)
        dataset_name_entry.grid(row=15, column=1, columnspan=2, padx=10, pady=5)
        additional_widgets.append(dataset_name_entry) #27

        partition_label = ttk.Label(root, text="What partition is the metric in?")
        partition_label.grid(row=16, column=0, padx=10, pady=10)
        additional_widgets.append(partition_label) #28

        streamcat_radio = ttk.Radiobutton(root, text="StreamCat", variable=selected_partition, value='streamcat')
        streamcat_radio.grid(row=16, column=1, sticky='w', padx=10, pady=5)
        lakecat_radio = ttk.Radiobutton(root, text="LakeCat", variable=selected_partition, value='lakecat')
        lakecat_radio.grid(row=16, column=2, sticky='w', padx=10, pady=5)
        additional_widgets.append(streamcat_radio) #29
        additional_widgets.append(lakecat_radio) #30

        submit_button.grid(row=18, column=0, columnspan=2, pady=10)
    
    elif choice =='Edit Metric Info':
        def get_edit_metric_info():
            if len(additional_widgets) > 0:
                #print(additional_widgets)
                for widget in additional_widgets:
                    #print(widget)
                    #print(type(widget))
                    widget.grid_forget()
            del additional_widgets[2:]
            table_name = 'sc_metrics_tg' if selected_partition.get() == 'streamcat' else 'lc_metrics_tg'
            tg_table = db_conn.metadata.tables[table_name]
            # metric_name_col = tg_table.c['metric_name']
            # metric_name_options = tg_table.c['metric_name'].keys()
            metric_name_options = db_conn.SelectColsFromTable('metric_name', table_name)

            metric_name_dropdown_var.set(metric_name_options[0])
            metric_name_dropdown = ttk.Combobox(root, textvariable=metric_name_dropdown_var, values=metric_name_options)
            metric_name_dropdown.grid(row=3, column=1, columnspan=3, padx=10, pady=5)
            additional_widgets.append(metric_name_dropdown) #2


            tg_columns = list(tg_table.c.keys())
            # tg_columns_dropdown_var = tk.StringVar(root)
            tg_columns_dropdown_var.set(tg_columns[0])
            tg_columns_dropdown = ttk.Combobox(root, textvariable=tg_columns_dropdown_var, values=tg_columns)
            tg_columns_dropdown.grid(row=4, column=1, columnspan=3, padx=10, pady=5)
            additional_widgets.append(tg_columns_dropdown) #3

            new_val_label = ttk.Label(root, text="Enter new value for the selected column:")
            new_val_label.grid(row=5, column=0, padx=10, pady=5)
            additional_widgets.append(new_val_label) #4
            new_val_entry = ttk.Entry(root)
            new_val_entry.grid(row=5, column=1, columnspan=2, padx=10, pady=5)
            additional_widgets.append(new_val_entry) #5

        streamcat_radio = ttk.Radiobutton(root, text="StreamCat", variable=selected_partition, value='streamcat', command=get_edit_metric_info)
        streamcat_radio.grid(row=2, column=1, sticky='w', padx=10, pady=5)
        lakecat_radio = ttk.Radiobutton(root, text="LakeCat", variable=selected_partition, value='lakecat', command=get_edit_metric_info)
        lakecat_radio.grid(row=2, column=2, sticky='w', padx=10, pady=5)
        additional_widgets.append(streamcat_radio) #0
        additional_widgets.append(lakecat_radio) #1
        # streamcat_radio.bind('<Button-1>', command=get_edit_metric_info)
        # lakecat_radio.bind('<Button-1>', command=get_edit_metric_info)
        # metric_name_dropdown_var = tk.StringVar(root)
        # Get TG table for appropriate partition
        # tg_table = db_conn.metadata.tables[selected_partition.get()]
        # metric_name_options = tg_table.c['metric_name']

        # metric_name_dropdown_var.set(table_options[0])
        # metric_name_dropdown = ttk.Combobox(root, textvariable=metric_name_dropdown_var, values=metric_name_options)
        # metric_name_dropdown.grid(row=3, column=1, columnspan=3, padx=10, pady=5)
        # additional_widgets.append(metric_name_dropdown) #2


        # tg_columns = list(tg_table.c.keys())
        # # tg_columns_dropdown_var = tk.StringVar(root)
        # tg_columns_dropdown_var.set(tg_columns[0])
        # tg_columns_dropdown = ttk.Combobox(root, textvariable=tg_columns_dropdown_var, values=tg_columns)
        # tg_columns_dropdown.grid(row=4, column=1, columnspan=3, padx=10, pady=5)
        # additional_widgets.append(tg_columns_dropdown) #3

        # new_val_label = ttk.Label(root, text="Enter new value for the selected column:")
        # new_val_label.grid(row=5, column=0, padx=10, pady=5)
        # additional_widgets.append(new_val_label) #4
        # new_val_entry = ttk.Entry(root)
        # new_val_entry.grid(row=5, column=1, columnspan=2, padx=10, pady=5)
        # additional_widgets.append(new_val_entry) #5

        submit_button.grid(row=7, column=0, columnspan=2, pady=10)


def choose_files(entry_widget):
    files = filedialog.askopenfilenames()
    entry_widget.delete(0, tk.END)
    entry_widget.insert(0, ', '.join(files))  # Displaying file paths in the entry

def submit():
    """After submit button is pressed get choice and execute appropriate database function.

    Returns: info popup displaying info about database action
    """

    # TODO should make sure these functions return some kind of info and use showinfo() popups to display the return results
    # TODO add progress bar
    choice = dropdown_var.get()
    if choice == 'Create Dataset from files':
        partition = selected_partition.get().lower()
        files = additional_widgets[4].get() #.cget('text').split(', ')  # Assuming file paths separated by commas
        ds_result, metric_result, display_result = db_conn.CreateDatasetFromFiles(partition, files)
        print(ds_result)
        print(metric_result)
        print(display_result)
        showinfo("Created Dataset", f"Results:\n Dataset Result: {ds_result} \n Metrics Inserted: {metric_result} \n New Display Names: {display_result}")

    elif choice == 'Create Table from File':
        name = additional_widgets[1].get()
        file = additional_widgets[3].get() # .cget('text')
        db_conn.CreateTableFromFile(name, file)

    elif choice == 'Rename Metric':
        old_name = additional_widgets[1].get()
        new_name = additional_widgets[3].get()
        db_conn.UpdateMetricName(old_name, new_name)

    elif choice == 'Activate/Deactivate Dataset':
        dataset_name = additional_widgets[0].get()
        db_conn.UpdateActiveDataset(dataset_name)

    elif choice == 'Update Table With File Data':
        table_name = table_name_dropdown_var.get()
        file = additional_widgets[3].get() # .cget('text')
        insert_results = db_conn.BulkInsertFromFile(table_name, file)
        showinfo(f"Updated - {table_name}", f"Inserted - {insert_results}")

    elif choice =='Create Metric Info':
        row_data = {}
        partition = selected_partition.get().lower()
        table_name = 'sc_metrics_tg' if partition == 'streamcat' else 'lc_metrics_tg'
        row_data['metric_name'] = additional_widgets[1].get()
        row_data['indicator_category'] = category_dropdown_var.get()
        row_data['aoi'] = aoi_dropdown_var.get()
        row_data['year'] = additional_widgets[7].get()
        row_data['webtool_name'] = additional_widgets[9].get()
        row_data['metric_description'] = additional_widgets[11].get()
        row_data['metric_units'] = additional_widgets[13].get()
        row_data['uuid'] = additional_widgets[15].get()
        row_data['metadata'] = additional_widgets[17].get()
        row_data['source_name'] = additional_widgets[19].get()
        row_data['source_url'] = additional_widgets[21].get()
        row_data['date_downloaded'] = additional_widgets[23].get()
        row_data['dsid'] = additional_widgets[25].get()
        row_data['final_table'] = additional_widgets[27].get()
        result = db_conn.InsertRow(table_name, row_data)
        showinfo(f"Created variable info for {row_data['metric_name']}", f"Inserted into {table_name}: {result}")
    
    elif choice =='Edit Metric Info':
        partition = selected_partition.get().lower()
        table_name = 'sc_metrics_tg' if partition == 'streamcat' else 'lc_metrics_tg'
        metric = metric_name_dropdown_var.get()
        col = tg_columns_dropdown_var.get()
        new_val = additional_widgets[5].get()
        print(table_name)
        print(col)
        print(metric)
        print(new_val)
        update = db_conn.UpdateRow(table_name, col, metric, new_val)

    else:
        showinfo('Invalid Selection', 'Please use the actions dropdown to select a valid option')
    
    changelog_update = simpledialog.askstring("Describe DB Change", "What changes did you make to the database here", parent = root)
    if changelog_update is not None:
        changelog_result = db_conn.newChangelogRow(selected_partition.get(), changelog_update)

if __name__ == "__main__":
    db_conn = DatabaseConnection()
    db_conn.connect()
    root = tk.Tk()
    root.title("StreamCat / LakeCat Database App")
    additional_widgets = []

    # Title
    ttk.Label(root, text="Database Action Selection", font=("Helvetica", 16)).grid(row=0, column=0, padx=10, pady=10)

    # Description
    ttk.Label(root, text="Select an action to perform with the database:", justify=tk.LEFT).grid(row=1, column=0, padx=10, pady=5)

    execute_sql_var = tk.BooleanVar()
    execute_sql_var.set(db_conn.execute)  # Set initial state based on current db_conn.execute_sql

    # Create the Checkbutton
    execute_sql_switch = ttk.Checkbutton(root, text="Execute SQL?", variable=execute_sql_var, command=lambda: setattr(db_conn, 'execute', execute_sql_var.get()))
    execute_sql_switch.grid(row=0, column=3, columnspan=2, pady=10)

    selected_partition = tk.StringVar(value='streamcat')

    # Dropdown selection
    dropdown_var = tk.StringVar(root)
    actions = ['Select Action', 'Create Dataset from files', 'Create Table from Files', 'Rename Metric', 'Activate/Deactivate Dataset', 'Update Table With File Data', 'Create Metric Info', 'Edit Metric Info']
    dropdown_var.set(actions[0])  # default value
    dropdown = ttk.Combobox(root, textvariable=dropdown_var, values=actions)
    dropdown.bind("<<ComboboxSelected>>", on_action_select)
    dropdown.grid(row=1, column=1, columnspan=3, padx=10, pady=5)

    # Define table name dropdown vars
    table_name_dropdown_var = tk.StringVar(root)
    table_options = db_conn.metadata.sorted_tables
    table_name_dropdown_var.set(table_options[0])  # default value

    # metrics tg categories
    category_dropdown_var = tk.StringVar(root)
    categories = ["Base", "Natural", "Anthropogenic"]
    category_dropdown_var.set(categories[0])

    # aoi dropdown
    aoi_dropdown_var = tk.StringVar(root)
    aois = ["Cat", "Ws", "CatRp100", "WsRp100", "Other"]
    aoi_dropdown_var.set(aois[-1])

    # Submit Button
    submit_button = ttk.Button(root, text="Submit", command=submit)
    submit_button.grid(row=5, column=0, columnspan=2, pady=10)

    metric_name_dropdown_var = tk.StringVar(root)
    tg_columns_dropdown_var = tk.StringVar(root)

    root.mainloop()
