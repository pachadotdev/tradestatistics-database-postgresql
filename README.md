# Open Trade Statistics Database (PostgreSQL)

Here I'm using R (data.table) to organize the [USITC](https://www.usitc.gov/data/gravity/dgd.htm)
datasets using 3rd normal form for fast querying.

All the data is publicly available a far as I know. I didn't need to create an account nor
use special credentials.

Besides some cleaning to keep common cases (identified by ISO code pairs) between different tables,
I added some minimal corrections to gravity variables data I detailed [here](https://github.com/pachadotdev/gravitydatasets).
