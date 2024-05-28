# Team 69
# Dillon Blake 1524907
# Andrea Delahaye 1424289
# Yue Peng 958289
# Jeff Phan 1577799
# Alistair Wilcox 212544

POST /_sql?format=txt
{
 "query": "SELECT * FROM students WHERE end > {oldest_start_new_data} "
}
