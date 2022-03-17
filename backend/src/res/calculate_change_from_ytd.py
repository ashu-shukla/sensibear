# import yesterdays_data_from_db as ytd
from res.yesterdays_data_from_db import get_yesterdays_data


# Gets the last date of data entered in DB, the OI data of the date and list of  dates.
last_date, recent_oi_data, list_of_dates = get_yesterdays_data()


# Function to calculate Interday changes.
def previous_data_compare(new_data):
    new_oi_data = new_data['oi']
    participants = recent_oi_data.keys()
    # Calculating and Adding data of inter-day changes to all the participants.
    for participant in participants:
        recent_derivatives = recent_oi_data[participant]
        new_derivatives = new_oi_data[participant]
        derivatives_keys = recent_derivatives.keys()
        # For FUT,Call and Put of every participant.
        for derivative in derivatives_keys:
            new_derivatives[derivative]['interday_change_in_long_oi'] = new_derivatives[derivative]['long_oi'] - \
                recent_derivatives[derivative]['long_oi']
            # Percentage change, if divide by zero error then 0 as of now.
            new_derivatives[derivative]['interday_percentage_change_in_long_oi'] = (
                new_derivatives[derivative]['interday_change_in_long_oi'] / recent_derivatives[derivative]['long_oi'])*100 if recent_derivatives[derivative]['long_oi'] else 0
            new_derivatives[derivative]['interday_change_in_short_oi'] = new_derivatives[derivative]['short_oi'] - \
                recent_derivatives[derivative]['short_oi']
            new_derivatives[derivative]['interday_percentage_change_in_short_oi'] = (
                new_derivatives[derivative]['interday_change_in_short_oi'] / recent_derivatives[derivative]['short_oi'])*100 if recent_derivatives[derivative]['short_oi'] else 0
            new_derivatives[derivative]['interday_change_in_net_oi'] = new_derivatives[derivative]['interday_change_in_long_oi'] - \
                new_derivatives[derivative]['interday_change_in_short_oi']
    return new_data
