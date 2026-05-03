# Card Tracker Overhaul

## What changed

1. Inventory is now the source of truth for sold data.
   - New inventory columns include `sold_date`, `sold_price`, `fees`, `shipping_charged`, `fees_total`, `net_proceeds`, `profit`, `sale_channel`, `show_id`, `show_name`, and `sold_transaction_id`.
   - Show sales write directly to the inventory row and mark it `SOLD`.
   - The Dashboard reads sales from inventory first and only uses legacy transactions as a fallback for inventory IDs that have not been migrated.

2. Show Sales Sync no longer relies on the transactions sheet.
   - Rows with a `sell_price` in the Shows page update inventory sale fields directly.
   - Snapshot rows still keep `sold_price`, `synced_at`, and `synced_transaction_id`.

3. Transactions page is preserved for online/listing workflows.
   - Online sales and trade-ins now also write sold fields back to inventory.
   - Header cleanup was added to remove blank duplicate transaction columns, such as duplicate `purchase_total`, `Grading Fee`, and `All In Cost` columns.

4. Misc now has mileage tracking.
   - Adds a separate `mileage` worksheet by default.
   - Tracks trip date, show/trip name, business purpose, locations, miles, parking/tolls, and notes.

5. One-time migration page added.
   - `pages/0_Migration_InventorySales.py`
   - Creates backups of inventory and transactions before writing.
   - Copies historical SOLD transactions to inventory rows.

## Deployment steps

1. Back up your Google Sheet manually first.
2. Replace the matching files in your Streamlit repo with these files.
3. Deploy/push to GitHub.
4. Open the new migration page in the app.
5. Click `Preview Migration`.
6. Review the rows that will update.
7. Click `Run Migration Now`.
8. Confirm Dashboard sales now match.
9. After confirming, remove `pages/0_Migration_InventorySales.py` from your repo so it cannot be run again accidentally.

## Important note

The old `transactions` worksheet is not deleted. It remains as a historical backup / online-listing workflow table. The new Dashboard logic prevents double counting by using inventory sale rows first and only falling back to legacy transactions where no inventory sale row exists.
