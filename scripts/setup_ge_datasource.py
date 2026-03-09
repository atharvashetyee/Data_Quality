# from great_expectations.data_context import DataContext

# context = DataContext()

# # Create or update Pandas runtime datasource
# datasource = context.sources.add_or_update_pandas(
#     name="pandas_runtime"
# )

# print("✅ Pandas runtime datasource ready")

# # Ensure runtime DataFrame asset exists
# asset_names = [asset.name for asset in datasource.assets]

# if "csv_chunk" not in asset_names:
#     datasource.add_dataframe_asset(name="csv_chunk")
#     print("✅ Runtime DataFrame asset 'csv_chunk' created")
# else:
#     print("ℹ️ Runtime DataFrame asset 'csv_chunk' already exists")
