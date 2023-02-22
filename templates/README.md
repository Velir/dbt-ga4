This folder contains useful templates that may aid in your GA4 data modeling work.

# Creating Custom Events

The dbt-ga4 package contains event models for many common events such as `page_view` and `file_donwload`. However, it's very common for a GA4 implementation to deploy custom events as well. The `my_custom_event.sql` file contains a template that can be used to create your own custom event model.

Steps to creating your own custom event model:
- Copy the content of template file, my_custom_event.sql
- Create a new .sql file to contain your model. It's best to include the event name in the model name. Ex: stg_event_my_custom_event.sql
- Paste the template content into your .sql file
- Replace the text `my_custom_event` with the actual name of your event. This must match your event name exactly as seen in the where clause: `where event_name = 'my_custom_event'`



## Adding Custom Parameters to Custom Events