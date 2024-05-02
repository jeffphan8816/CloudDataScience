POST /_sql?format=txt
{
 "query": "SELECT * FROM crashes WHERE end > {oldest_start_new_data} "
}
