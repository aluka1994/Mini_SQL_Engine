#include <iostream>
#include <bits/stdc++.h>
#include <string>

using namespace std;


typedef map < string , map < string , vector < int > > > dbt;
typedef map < string , map < int , string > > dbref;


typedef struct {

	dbt data;
	dbref ref;

}tdb;


typedef struct {
	vector< string > columns;
	vector< string > tables;
	vector< string > comparisions;
	vector< string > operators;
	vector< string > agg;
}query;


typedef struct {
	int total_tables;
	dbt mydb;
	string FinalQuery;
	map < string , int > col_to_index;
	vector < string > table_order;
	vector < int > table_col;
	vector < int > table_sizes;
	vector < vector < string > > columns;
	vector < int > row_pointer;
	vector < int > col_pointer;
	vector < int > candidate;
}minedata;

//global
bool error = false;
vector < vector < int > > join_table;
vector < vector < int > > join_tables;
set< string > to_remove; //for distinct queries it is used
set< string > toRemove;
double g_avg=0;
long long int totalsum=0;
long long int g_sum=0;
long long int minimumvalue;
long long int g_min=INT_MAX;
long long int maximumvalue;
long long int g_max=INT_MIN;

tdb * rmdata(){
	ifstream mfile("metadata.txt");
	bool flag;
	flag=0;
	string metadata;
	string tablename;
	dbt data;
	dbref dataref;
	int col_count;
	col_count=1;
	while(mfile>>metadata){
		if(flag==1){
			if(metadata[0]!='<'){
				vector < int > temp;
				temp.clear();
				data[tablename][metadata]=temp;
				dataref[tablename][col_count]=metadata;
				col_count=col_count+1;
				
			}
			else if(metadata[0]=='<'){
				flag=0;
			}
		}
		else{
			if(metadata[0]=='<'){
				flag=1;
			}
			map < string , vector < int > > temp3;
			mfile>>tablename;
			temp3.clear();
			map < string , vector < int > > temp1;
			temp3.clear();
			temp1.clear();
			data[tablename]=temp1;
			map < int , string > temp2;
			temp2.clear();
			temp3.clear();
			dataref[tablename]=temp2;
			col_count=1;
			temp3.clear();
		}
	}
	tdb * mydb = new tdb;
	mydb->data=data;
	mydb->ref=dataref;
	mfile.close();
	return mydb;
}

tdb * popdb(tdb * mydb){
	string itemer;
	dbt::iterator it;
	int col_count;
	col_count=1;
	string filename;
	string linedata;
	string int_data;
	it=(mydb->data).begin();
	while(it!=(mydb->data).end()){
		filename="";
		string filename1;
		int_data="";
		filename1="";
		filename.append(it->first);
		filename1.append(".csv");
		filename.append(".csv");
		ifstream datafile(filename);
		stringstream mystr1;
		while(datafile>>linedata){
		
			linedata.erase(remove(linedata.begin(),linedata.end(),'\"'),linedata.end()); //remove all quotes
			replace (linedata.begin(), linedata.end(), ',' , ' ');
			stringstream linestream(linedata);
			col_count=1;
			while(linestream>>int_data)
			{
				mydb->data[it->first][((mydb->ref)[it->first][col_count])].push_back(stoi(int_data));
				col_count++;
			}
		}
		datafile.close();
		it++;
	}
	return mydb;
}
query * parseit(string input)
{
	query * myquery=new query;
	int query_len;
	stringstream inputstream1;
	query_len=input.size();
	stringstream append1;
	replace (input.begin(), input.end(), ',' , ' ');
	stringstream input_stream;
	stringstream outputstream;
	input_stream<<input;
	outputstream<<input;
	string token;
	string between;
	between="";
	string tok_query;
	tok_query="";
	int identifier;
	identifier=-1;
	while(input_stream>>token)
	{
		if(token.compare("select")==0){
			identifier++;
			continue;
		}
		else if(token.compare("SELECT")==0){
			identifier++;
			continue;
		}
		else if(token.compare("FORM")==0){
			identifier++;
			continue;
		}
		else if(token.compare("from")==0){
			identifier++;
			continue;
		}
		else if(token.compare("WHERE")==0){
			identifier++;
			continue;
		}
		else if(token.compare("where")==0){
			identifier++;
			continue;
		}
		switch(identifier)
		{
			case 0:
				{
					(myquery->columns).push_back(token);
					break;
				}
			case 1:
				{
					(myquery->tables).push_back(token);
					break;
				}
			case 2:
				{
					if(token=="AND")
					{
						token="and";
					}
					else  if(token=="OR")
					{
						token="or";
					}
					if(token=="and")
					{
						(myquery->operators).push_back(token);
						if(between.compare("")!=0)
						{
							string s1,s2,s3,s4;
							s1="";
							s2="";
							s3="";
							s4="";
							int check ;
							int check2;
							check =0;
							check2=0;
							int i;
							i=0;
							while(i<between.size())
							{
								if(between[i]=='<')
								{	
									check=1; 
								}
								else if(between[i]=='>'){
									check=1; 
								}
								else if(between[i]=='='){
									check=1; 
								}
								else if(between[i]=='!'){
									check=1; 
								}
								else if(check==1)
								{
									if(between[i]!='=')
										check=2;
								}	
								if(check==0 )
								{	
									if(between[i]!=' ')
										s1+=between[i]; //
								}
								if(check==1  )
								{
									if(between[i]!=' ')
									s2+=between[i];
								}
								if(check==2 )
								{	
									if(between[i]!=' ')
										s3+=between[i];
								}
								tok_query=s1+" "+s2+" "+s3;
								i++;
							}
							(myquery->comparisions).push_back(tok_query);
						}
						between="";
						tok_query="";
						continue;
					}
					else if(token=="or"){
						(myquery->operators).push_back(token);
						if(between.compare("")!=0)
						{
							string s1,s2,s3,s4;
							s1="";
							s2="";
							s3="";
							s4="";
							int check ;
							int check2;
							check =0;
							check2=0;
							int i;
							i=0;
							while(i<between.size())
							{
								if(between[i]=='<')
								{	
									check=1; 
								}
								else if(between[i]=='>'){
									check=1; 
								}
								else if(between[i]=='='){
									check=1; 
								}
								else if(between[i]=='!'){
									check=1; 
								}
								else if(check==1)
								{
									if(between[i]!='=')
										check=2;
								}	
								if(check==0 )
								{	
									if(between[i]!=' ')
										s1+=between[i]; //
								}
								if(check==1  )
								{
									if(between[i]!=' ')
									s2+=between[i];
								}
								if(check==2 )
								{	
									if(between[i]!=' ')
										s3+=between[i];
								}
								tok_query=s1+" "+s2+" "+s3;
								i++;
							}
							(myquery->comparisions).push_back(tok_query);
						}
						between="";
						tok_query="";
						continue;

					}
					between+=token;
					between+=" ";
					break;
				}
			default:
				break;
		}
	}
	if(between!="")
	{
		string s1,s2,s3,s4;
		s1="";
		s2="";
		s3="";
		s4="";
		int check ;
		int check2;
		check =0;
		check2=0;
		int i;
		i=0;
		while(i<between.size())
		{
			if(between[i]=='<')
			{	
				check=1; 
			}
			else if(between[i]=='>'){
				check=1; 
			}
			else if(between[i]=='='){
				check=1; 
			}
			else if(between[i]=='!'){
				check=1; 
			}
			else if(check==1)
			{
				if(between[i]!='=')
					check=2;
			}	
			if(check==0 )
			{	
				if(between[i]!=' ')
					s1+=between[i]; //
			}
			if(check==1  )
			{
				if(between[i]!=' ')
					s2+=between[i];
			}
			if(check==2 )
			{	
				if(between[i]!=' ')
					s3+=between[i];
			}
			tok_query=s1+" "+s2+" "+s3;
			i++;
		}
		(myquery->comparisions).push_back(tok_query);
	}
	return myquery;
}


query * parseiit(string input){
	query * myquery = new query;
	int queryLength;
	queryLength = input.size();
	replace (input.begin(),input.end(),',',',');
	stringstream inputStream;
	stringstream outputstream;
	inputStream << input;
	outputstream << input;
	string token;
	int querying;
	string between;
	querying=0;
	between="";
	string tokenQuery;
	tokenQuery="";
	int indentify;
	indentify = -1;
	while(inputStream >> token){
		if(token.compare("select")==0){
			indentify= indentify+1;
			continue;
		}
		else if(token.compare("SELECT")==0){
			indentify= indentify+1;
			continue;
		}
		else if(token.compare("FORM")==0){
			indentify= indentify+1;
			continue;
		}
		else if(token.compare("from")==0){
			indentify= indentify+1;
			continue;
		}
		else if(token.compare("WHERE")==0){
			indentify= indentify+1;
			continue;
		}
		else if(token.compare("where")==0){
			indentify= indentify+1;
			continue;
		}

		switch(indentify){

			case 0: 
				{
					(myquery->columns).push_back(token);
					break;
				}
			case 1:
				{
					(myquery->tables).push_back(token);
				}
			case 2:
				{
					if(token=="OR")
					{
						token="or";
					}
					else if(token=="AND"){
						token="and";
					}
					if(token=="and" or token == "or")
					{
						string compared;
						(myquery->operators).push_back(token);
						if(between.compare("")!=0)
						{
							string s1,s2,s3,s4;
							s1="";
							s2="";
							s3="";
							s4="";
							int check ;
							int check2;
							check =0;
							check2=0;
							int i;
							while(i<between.size())
							{
								if(between[i]=='<')
								{	
									check=1; 
								}
								else if(between[i]=='>'){
									check=1; 
								}
								else if(between[i]=='='){
									check=1; 
								}
								else if(between[i]=='!'){
									check=1; 
								}
								else if(check==1)
								{
									if(between[i]!='=')
									    check=2;
								}	 
								if(check==0  )
								{
									if(between[i]!=' ')
										s1+=between[i];
								}	 
								if(check==1 )
								{	
									if(between[i]!=' ')
										s2+=between[i];
								}	
								if(check==2 )
								{
									if(between[i]!=' ')
										s3+=between[i];
								}	
								tokenQuery = s1+" "+s2+" "+s3;
								i++;
							}
							(myquery->comparisions).push_back(tokenQuery);
						}
					between = "";
					tokenQuery="";
					continue;
					}
				between=between+token;
				between=between+" ";
				break;
				}
			default:
					break;
		}
	}
if(between!="")
	{
		string s1,s2,s3,s4;
		s1="";
		s2="";
		s3="";
		s4="";
		int check ;
		int check2;
		check =0;
		check2=0;
		int i;
		i=0;
		while(i<between.size())
		{
			if(between[i]=='<')
			{	
				check=1; 
			}
			else if(between[i]=='>'){
				check=1; 
			}
			else if(between[i]=='='){
				check=1; 
			}
			else if(between[i]=='!'){
				check=1; 
			}
			else if(check==1)
			{
				if(between[i]!='=')
					check=2;
			}	
			if(check==0 )
			{	
				if(between[i]!=' ')
					s1+=between[i]; //
			}
			if(check==1  )
			{
				if(between[i]!=' ')
					s2+=between[i];
			}
			if(check==2 )
			{	
				if(between[i]!=' ')
					s3+=between[i];
			}
			tokenQuery=s1+" "+s2+" "+s3;
			i++;
		}
		(myquery->comparisions).push_back(tokenQuery);
	}
	return myquery;
}

bool is_number(string input)
{
	string input1;
	int all1;
	if(input[0]=='+')
	{
		int i=1;
		while(i<input.size())
		{
			if(!isdigit(input[i])){

				return false;
			}
			i++;
		}
	}
	else if(input[0]=='-'){
		int i=1;
		while(i<input.size())
		{
			if(!isdigit(input[i])){

				return false;
			}
			i++;
		}
	}
	else if(isdigit(input[0])){
		int i=1;
		while(i<input.size())
		{
			if(!isdigit(input[i])){

				return false;
			}
			i++;
		}
	}
	else 
	{
		return false;
	}

	return true;
}

query * verify_wrapper(tdb * mydb,query * inputQuery,string currentColumn,int indexString,bool flag){
		size_t foundPos;
		size_t foundpostion;
		foundPos = currentColumn.find("(");
		if(foundPos==string::npos){
			inputQuery->agg.push_back("");
		}
		else{
			string colss;
			inputQuery->agg.push_back(currentColumn.substr(0,foundPos));
			int columnIndexs;
			currentColumn = currentColumn.substr(foundPos+1);
			string currentcol1 = currentColumn;
			foundPos=currentColumn.find(")");
			size_t foundpostion2;
			foundpostion2 = foundPos;
			currentColumn=currentColumn.substr(0,foundPos);//chnage
		}

		foundPos=currentColumn.find(".");
		size_t foundpostion3;
		if(foundPos!=string::npos){
			foundpostion3 = foundPos;
			int r3;
			r3=0;
			if(mydb->data[currentColumn.substr(r3,foundPos)].find(currentColumn.substr(foundPos+1))==mydb->data[currentColumn.substr(r3,foundPos)].end()){
				string error2;
				error=true;
			}
			else if(mydb->data.find(currentColumn.substr(0,foundPos))==mydb->data.end()){
				string error1;
				error=true;
			}
		}
		else{
			int count;
			count=0;
			string newcolumname,token,mycompare;
			newcolumname="";
			token="";
			mycompare="";
			int i;
			i=0;
			while(i<(inputQuery->tables).size()){
				string columnindexname;
				if(mydb->data[inputQuery->tables[i]].find(currentColumn)!=mydb->data[inputQuery->tables[i]].end()){
					int count2=0;
					count2=count2+1;
					count=count+1;
					if(flag){
						count2++;
						newcolumname="";
						string newcolumname1;
						newcolumname=newcolumname+inputQuery->tables[i]+"."+currentColumn;
						stringstream newcolumname2;
						inputQuery->columns[indexString]=newcolumname;
					}
					else if(flag==0){
						newcolumname="";
						token="";
						stringstream comp;
						newcolumname=newcolumname+inputQuery->tables[i]+"."+currentColumn;
						stringstream compare1;
						mycompare=inputQuery->comparisions[indexString];
						string discols;
						inputQuery->comparisions[indexString]="";
						stringstream myquery1;
						comp<<mycompare;
						while(comp>>token){
							if(token.compare(currentColumn)!=0){
								inputQuery->comparisions[indexString]=inputQuery->comparisions[indexString]+token+" ";

							}
							else if(token.compare(currentColumn)==0)
							{
								inputQuery->comparisions[indexString]=inputQuery->comparisions[indexString]+newcolumname+" ";
								
							}
						}
						inputQuery->comparisions[indexString].pop_back();
					}
				}
				i++;
			}
			if(count!=1){
				error=true;
				return inputQuery;
			}
		}
		return inputQuery;
}


query * verifyquery(tdb * mydb, query *inputQuery){
	map<string ,vector <int> >::iterator colit2;
	if(inputQuery->columns.size()==1 and inputQuery->columns[0].compare("*")==0){
		int a,b;
		inputQuery->columns.clear();
		string newcolumns;
		newcolumns="";
		map<string ,vector <int> >::iterator colit;
		int i;
		i=0;
		while(i<((inputQuery->tables).size())){
			colit=mydb->data[inputQuery->tables[i]].begin();
			while(colit!=mydb->data[inputQuery->tables[i]].end()){
				newcolumns="";
				newcolumns=newcolumns+inputQuery->tables[i]+"."+colit->first;
				inputQuery->columns.push_back(newcolumns);
				colit++;
			}
			i++;
		}
	}
	int i=0;
	while(i<((inputQuery->tables).size()))
	{
		stringstream mydb1;
		if(mydb->data.find(inputQuery->tables[i])==mydb->data.end())
		{	
			error=true;
			return inputQuery;
		}
		i++;
	}
	string currentColumn;
	//for input column check
	i=0;
	while(i<(inputQuery->columns).size())
	{
		currentColumn=inputQuery->columns[i];
		string data1;
		inputQuery=verify_wrapper(mydb,inputQuery,currentColumn,i,true);
		if(error==true){
			return inputQuery;
		}
		i++;
	}
	int count1;
	count1=0;
	string token;
	token="";
	i=0;
	while(i<inputQuery->comparisions.size())
	{
		currentColumn=inputQuery->comparisions[i];
		stringstream comp;
		token="";
		comp<<currentColumn;
		count1=0;
		while(comp>>token)
		{
			string fixed1;
			if((count1==0) or (count1==2))
			{
				if(!is_number(token))
				{
					stringstream comparetable;
					inputQuery=verify_wrapper(mydb,inputQuery,token,i,false);
					if(error==true){
						return inputQuery;
					}
				}
			}
			token="";
			count1=count1+1;
		}
		i++;
	}
	return inputQuery;
}

minedata * popmine(query * query_parsed, tdb * mydb)
{
	minedata * mymine=new minedata;
	vector < string > tabcols;
	string currenttable;
	int col_count;
	col_count=0;
	int i;
	i=0;
	while(i<query_parsed->tables.size()){
		currenttable=query_parsed->tables[i];
		tabcols.clear();
		mymine->table_order.push_back(currenttable);
		string table2;
		map < string , vector < int > >:: iterator item;
		stringstream mystr1;
		item=mydb->data[currenttable].begin();
		while(item!=mydb->data[currenttable].end())
		{
			stringstream all2;
			tabcols.push_back(item->first);
			string tabcolumnd;
			mymine->col_to_index[currenttable+"."+item->first]=col_count;
			col_count=col_count+1;
			item++;
		}
		string rad;
		mymine->columns.push_back(tabcols);
		stringstream mystr;
		item--;
		mymine->table_sizes.push_back(mydb->data[currenttable][item->first].size());
		i++;
	}
	
	mymine->total_tables=mymine->table_sizes.size();
	int k1,k2;
	k1=0;
	k2=0;
	while(k1<mymine->total_tables){
		mymine->row_pointer.push_back(0);
		k1++;
	}
	while(k2<col_count){
		mymine->candidate.push_back(0);
		k2++;
	}
	
	return mymine;
}

bool sqlFunctions(string agg_func,tdb * mydb,query * query_parsed)
{
	if(agg_func=="avg" )
	{
		g_avg=0;
		string c_name;
		c_name=query_parsed->columns[0];
		size_t found_pos;
		found_pos=c_name.find("."); 
		c_name=c_name.substr(found_pos+1);
		string t_name;
		t_name=query_parsed->tables[0];
		vector< int > rel_data;
		rel_data = mydb->data[t_name][c_name];
		int i;
		i=0;
		while(i<rel_data.size())
		{
			if(g_max >=rel_data[i]){
				g_max=g_max;
			}
			else{
				g_max=rel_data[i];
			}
			//g_max=g_max>=rel_data[i]?g_max:rel_data[i];
			if(g_min >=rel_data[i]){
				g_min=g_min;
			}
			else{
				g_min=rel_data[i];
			}
			g_avg=g_avg+rel_data[i];
			//g_min=g_min<=rel_data[i]?g_min:rel_data[i];
			//g_avg+=rel_data[i];
			i++;
		}
		g_sum=g_avg;
		g_avg=g_avg/(1.0*rel_data.size());
		cout<<agg_func<<"("<<query_parsed->columns[0]<<")"<<endl;
		if(agg_func=="max")
		{
			cout<<g_max<<endl;
		}
		else if(agg_func=="min")
		{
			cout<<g_min<<endl;
		}
		else if(agg_func=="avg")
		{
			cout.setf(ios::fixed);
			// setprecision is for limiting decimal value.
			cout<<setprecision(4)<<g_avg<<endl;
		}
		else if(agg_func=="sum")
		{
			cout<<g_sum<<endl;
		}
		return true;
	}
	else if(agg_func=="sum"){
		g_avg=0;
		string c_name;
		c_name=query_parsed->columns[0];
		size_t found_pos;
		found_pos=c_name.find("."); 
		c_name=c_name.substr(found_pos+1);
		string t_name;
		t_name=query_parsed->tables[0];
		vector< int > rel_data;
		rel_data = mydb->data[t_name][c_name];
		int i;
		i=0;
		while(i<rel_data.size())
		{
			if(g_max >=rel_data[i]){
				g_max=g_max;
			}
			else{
				g_max=rel_data[i];
			}
			//g_max=g_max>=rel_data[i]?g_max:rel_data[i];
			if(g_min >=rel_data[i]){
				g_min=g_min;
			}
			else{
				g_min=rel_data[i];
			}
			g_avg=g_avg+rel_data[i];
			//g_min=g_min<=rel_data[i]?g_min:rel_data[i];
			//g_avg+=rel_data[i];
			i++;
		}
		g_sum=g_avg;
		g_avg=g_avg/(1.0*rel_data.size());
		cout<<agg_func<<"("<<query_parsed->columns[0]<<")"<<endl;
		if(agg_func=="max")
		{
			cout<<g_max<<endl;
		}
		else if(agg_func=="min")
		{
			cout<<g_min<<endl;
		}
		else if(agg_func=="avg")
		{
			cout.setf(ios::fixed);
			// setprecision is for limiting decimal value.
			cout<<setprecision(4)<<g_avg<<endl;
		}
		else if(agg_func=="sum")
		{
			cout<<g_sum<<endl;
		}
		return true;
	}
	else if(agg_func=="max"){
		g_avg=0;
		string c_name;
		c_name=query_parsed->columns[0];
		size_t found_pos;
		found_pos=c_name.find("."); 
		c_name=c_name.substr(found_pos+1);
		string t_name;
		t_name=query_parsed->tables[0];
		vector< int > rel_data;
		rel_data = mydb->data[t_name][c_name];
		int i;
		i=0;
		while(i<rel_data.size())
		{
			if(g_max >=rel_data[i]){
				g_max=g_max;
			}
			else{
				g_max=rel_data[i];
			}
			//g_max=g_max>=rel_data[i]?g_max:rel_data[i];
			if(g_min >=rel_data[i]){
				g_min=g_min;
			}
			else{
				g_min=rel_data[i];
			}
			g_avg=g_avg+rel_data[i];
			//g_min=g_min<=rel_data[i]?g_min:rel_data[i];
			//g_avg+=rel_data[i];
			i++;
		}
		g_sum=g_avg;
		g_avg=g_avg/(1.0*rel_data.size());
		cout<<agg_func<<"("<<query_parsed->columns[0]<<")"<<endl;
		if(agg_func=="max")
		{
			cout<<g_max<<endl;
		}
		else if(agg_func=="min")
		{
			cout<<g_min<<endl;
		}
		else if(agg_func=="avg")
		{
			cout.setf(ios::fixed);
			// setprecision is for limiting decimal value.
			cout<<setprecision(4)<<g_avg<<endl;
		}
		else if(agg_func=="sum")
		{
			cout<<g_sum<<endl;
		}
		return true;
	}
	else if(agg_func=="min"){
		g_avg=0;
		string c_name;
		c_name=query_parsed->columns[0];
		size_t found_pos;
		found_pos=c_name.find("."); 
		c_name=c_name.substr(found_pos+1);
		string t_name;
		t_name=query_parsed->tables[0];
		vector< int > rel_data;
		rel_data = mydb->data[t_name][c_name];
		int i;
		i=0;
		while(i<rel_data.size())
		{
			if(g_max >=rel_data[i]){
				g_max=g_max;
			}
			else{
				g_max=rel_data[i];
			}
			//g_max=g_max>=rel_data[i]?g_max:rel_data[i];
			if(g_min >=rel_data[i]){
				g_min=g_min;
			}
			else{
				g_min=rel_data[i];
			}
			g_avg=g_avg+rel_data[i];
			//g_min=g_min<=rel_data[i]?g_min:rel_data[i];
			//g_avg+=rel_data[i];
			i++;
		}
		g_sum=g_avg;
		g_avg=g_avg/(1.0*rel_data.size());
		cout<<agg_func<<"("<<query_parsed->columns[0]<<")"<<endl;
		if(agg_func=="max")
		{
			cout<<g_max<<endl;
		}
		else if(agg_func=="min")
		{
			cout<<g_min<<endl;
		}
		else if(agg_func=="avg")
		{
			cout.setf(ios::fixed);
			// setprecision is for limiting decimal value.
			cout<<setprecision(4)<<g_avg<<endl;
		}
		else if(agg_func=="sum")
		{
			cout<<g_sum<<endl;
		}
		return true;
	}
	return false;
}

bool agg_func_handlerwq(string agg_func,tdb *mydb,query * query_parsed){

	if(agg_func=="avg" )
	{
		g_avg=0;
		string c_name;
		c_name=query_parsed->columns[0];
		size_t found_pos;
		found_pos=c_name.find("."); 
		c_name=c_name.substr(found_pos+1);
		string t_name;
		t_name=query_parsed->tables[0];
		vector< int > rel_data;
		rel_data = mydb->data[t_name][c_name];
		int i;
		i=0;
		while(i<rel_data.size())
		{
			if(g_max >=rel_data[i]){
				g_max=g_max;
			}
			else{
				g_max=rel_data[i];
			}
			//g_max=g_max>=rel_data[i]?g_max:rel_data[i];
			if(g_min >=rel_data[i]){
				g_min=g_min;
			}
			else{
				g_min=rel_data[i];
			}
			g_avg=g_avg+rel_data[i];
			//g_min=g_min<=rel_data[i]?g_min:rel_data[i];
			//g_avg+=rel_data[i];
			i++;
		}
		g_sum=g_avg;
		g_avg=g_avg/(1.0*rel_data.size());
		cout<<agg_func<<"("<<query_parsed->columns[0]<<")"<<endl;
		if(agg_func=="max")
		{
			cout<<g_max<<endl;
		}
		else if(agg_func=="min")
		{
			cout<<g_min<<endl;
		}
		else if(agg_func=="avg")
		{
			cout.setf(ios::fixed);
			// setprecision is for limiting decimal value.
			cout<<setprecision(4)<<g_avg<<endl;
		}
		else if(agg_func=="sum")
		{
			cout<<g_sum<<endl;
		}
		return true;
	}
	else if(agg_func=="sum"){
		g_avg=0;
		string c_name;
		c_name=query_parsed->columns[0];
		size_t found_pos;
		found_pos=c_name.find("."); 
		c_name=c_name.substr(found_pos+1);
		string t_name;
		t_name=query_parsed->tables[0];
		vector< int > rel_data;
		rel_data = mydb->data[t_name][c_name];
		int i;
		i=0;
		while(i<rel_data.size())
		{
			if(g_max >=rel_data[i]){
				g_max=g_max;
			}
			else{
				g_max=rel_data[i];
			}
			//g_max=g_max>=rel_data[i]?g_max:rel_data[i];
			if(g_min >=rel_data[i]){
				g_min=g_min;
			}
			else{
				g_min=rel_data[i];
			}
			g_avg=g_avg+rel_data[i];
			//g_min=g_min<=rel_data[i]?g_min:rel_data[i];
			//g_avg+=rel_data[i];
			i++;
		}
		g_sum=g_avg;
		g_avg=g_avg/(1.0*rel_data.size());
		cout<<agg_func<<"("<<query_parsed->columns[0]<<")"<<endl;
		if(agg_func=="max")
		{
			cout<<g_max<<endl;
		}
		else if(agg_func=="min")
		{
			cout<<g_min<<endl;
		}
		else if(agg_func=="avg")
		{
			cout.setf(ios::fixed);
			// setprecision is for limiting decimal value.
			cout<<setprecision(4)<<g_avg<<endl;
		}
		else if(agg_func=="sum")
		{
			cout<<g_sum<<endl;
		}
		return true;
	}
	else if(agg_func=="max"){
		g_avg=0;
		string c_name;
		c_name=query_parsed->columns[0];
		size_t found_pos;
		found_pos=c_name.find("."); 
		c_name=c_name.substr(found_pos+1);
		string t_name;
		t_name=query_parsed->tables[0];
		vector< int > rel_data;
		rel_data = mydb->data[t_name][c_name];
		int i;
		i=0;
		while(i<rel_data.size())
		{
			if(g_max >=rel_data[i]){
				g_max=g_max;
			}
			else{
				g_max=rel_data[i];
			}
			//g_max=g_max>=rel_data[i]?g_max:rel_data[i];
			if(g_min >=rel_data[i]){
				g_min=g_min;
			}
			else{
				g_min=rel_data[i];
			}
			g_avg=g_avg+rel_data[i];
			//g_min=g_min<=rel_data[i]?g_min:rel_data[i];
			//g_avg+=rel_data[i];
			i++;
		}
		g_sum=g_avg;
		g_avg=g_avg/(1.0*rel_data.size());
		cout<<agg_func<<"("<<query_parsed->columns[0]<<")"<<endl;
		if(agg_func=="max")
		{
			cout<<g_max<<endl;
		}
		else if(agg_func=="min")
		{
			cout<<g_min<<endl;
		}
		else if(agg_func=="avg")
		{
			cout.setf(ios::fixed);
			// setprecision is for limiting decimal value.
			cout<<setprecision(4)<<g_avg<<endl;
		}
		else if(agg_func=="sum")
		{
			cout<<g_sum<<endl;
		}
		return true;
	}
	else if(agg_func=="min"){
		g_avg=0;
		string c_name;
		c_name=query_parsed->columns[0];
		size_t found_pos;
		found_pos=c_name.find("."); 
		c_name=c_name.substr(found_pos+1);
		string t_name;
		t_name=query_parsed->tables[0];
		vector< int > rel_data;
		rel_data = mydb->data[t_name][c_name];
		int i;
		i=0;
		while(i<rel_data.size())
		{
			if(g_max >=rel_data[i]){
				g_max=g_max;
			}
			else{
				g_max=rel_data[i];
			}
			//g_max=g_max>=rel_data[i]?g_max:rel_data[i];
			if(g_min >=rel_data[i]){
				g_min=g_min;
			}
			else{
				g_min=rel_data[i];
			}
			g_avg=g_avg+rel_data[i];
			//g_min=g_min<=rel_data[i]?g_min:rel_data[i];
			//g_avg+=rel_data[i];
			i++;
		}
		g_sum=g_avg;
		g_avg=g_avg/(1.0*rel_data.size());
		cout<<agg_func<<"("<<query_parsed->columns[0]<<")"<<endl;
		if(agg_func=="max")
		{
			cout<<g_max<<endl;
		}
		else if(agg_func=="min")
		{
			cout<<g_min<<endl;
		}
		else if(agg_func=="avg")
		{
			cout.setf(ios::fixed);
			// setprecision is for limiting decimal value.
			cout<<setprecision(4)<<g_avg<<endl;
		}
		else if(agg_func=="sum")
		{
			cout<<g_sum<<endl;
		}
		return true;
	}
	return false;
}

bool checkWhere(minedata * mine1 , string oa , query * queryp){
	vector < int > cand(mine1->candidate);
	string tokenQuery,comp;
	stringstream cond;
	cond<<oa;
	vector < string > check ;
	while(cond >> tokenQuery){
		check.push_back(tokenQuery);
	}
	int a1,a2;
	comp = check[1];
	if(comp == "<"){
		if(is_number(check[0]))
		{
			a1=is_number(check[0]);
			if(atoi(check[0].c_str())<cand[mine1->col_to_index[check[2]]])
			{
				a1=is_number(check[0]);
				return true;
			}
		}
		else if(is_number(check[2]))
		{
			a1=is_number(check[2]);
			if(cand[mine1->col_to_index[check[0]]]<atoi(check[2].c_str()))
			{
				a1=is_number(check[2]);
				return true;
			}
		}
		else if(cand[mine1->col_to_index[check[0]]]<cand[mine1->col_to_index[check[2]]])
		{
			a1=is_number(check[0]);
			return true;
		}
	}
	else if(comp == "<="){
		if(is_number(check[0]))
		{
			a1=is_number(check[0]);
			if(atoi(check[0].c_str())<=cand[mine1->col_to_index[check[2]]])
			{
				a1=is_number(check[0]);
				return true;
			}
		}
		else if(is_number(check[2]))
		{
			a1=is_number(check[2]);
			if(cand[mine1->col_to_index[check[0]]]<=atoi(check[2].c_str()))
			{
				a1=is_number(check[2]);
				return true;
			}
		}
		else if(cand[mine1->col_to_index[check[0]]]<=cand[mine1->col_to_index[check[2]]])
		{	
			a1=is_number(check[0]);
			a2=is_number(check[2]);
			return true;
		}
	}
	else if(comp == ">=")
	{
		if(is_number(check[0]))
		{
			a1=is_number(check[0]);
			if(atoi(check[0].c_str())>=cand[mine1->col_to_index[check[2]]])
			{
				a1=is_number(check[0]);
				return true;
			}
		}
		else if(is_number(check[2]))
		{
			a1=is_number(check[2]);
			if(cand[mine1->col_to_index[check[0]]]>=atoi(check[2].c_str()))
			{
				a1=is_number(check[2]);
				return true;
			}
		}
		else if(cand[mine1->col_to_index[check[0]]]>=cand[mine1->col_to_index[check[2]]])
		{
			a1=is_number(check[0]);
			a2=is_number(check[2]);
			return true;
		}
	}
	else if(comp == "!=")
	{
		if(is_number(check[0]))
		{
			a1=is_number(check[0]);
			if(atoi(check[0].c_str())!=cand[mine1->col_to_index[check[2]]])
			{
				a1=is_number(check[0]);
				return true;
			}
		}
		else if(is_number(check[2]))
		{
			a1=is_number(check[2]);
			if(cand[mine1->col_to_index[check[0]]]!=atoi(check[2].c_str()))
			{
				a1=is_number(check[2]);
				return true;
			}
		}
		else if(cand[mine1->col_to_index[check[0]]]!=cand[mine1->col_to_index[check[2]]])
		{
			a1=is_number(check[0]);
			a2=is_number(check[2]);
			return true;
		}
	}
	else if(comp == ">")
	{
		if(is_number(check[0]))
		{
			a1=is_number(check[0]);
			if(atoi(check[0].c_str())>cand[mine1->col_to_index[check[2]]])
			{
				a1=is_number(check[0]);
				return true;
			}
		}
		else if(is_number(check[2]))
		{
			a1=is_number(check[2]);
			if(cand[mine1->col_to_index[check[0]]]>atoi(check[2].c_str()))
			{
				a1=is_number(check[2]);
				return true;
			}
		}
		else if(cand[mine1->col_to_index[check[0]]]>cand[mine1->col_to_index[check[2]]])
		{
			a1=is_number(check[0]);
			a2=is_number(check[2]);
			return true;
		}
	}
	else if(comp == "="){
		if(is_number(check[0]))
		{
			a1=is_number(check[0]);
			if(atoi(check[0].c_str())==cand[mine1->col_to_index[check[2]]])
			{
				a1=is_number(check[0]);
				return true;
			}
		}
		else if(is_number(check[2]))
		{
			a1=is_number(check[2]);
			if(cand[mine1->col_to_index[check[0]]]==atoi(check[2].c_str()))
			{
				a1=is_number(check[2]);
				return true;
			}
		}
		else if(cand[mine1->col_to_index[check[0]]]==cand[mine1->col_to_index[check[2]]])
		{
			a1=is_number(check[0]);
			a2=is_number(check[2]);
			//removing duplicate column
			to_remove.insert(check[2]);
			return true;
		}
	}
	return false;
}

bool acceptOnWhere(minedata * mine1 , query * queryp){
	if(queryp->comparisions.size()==0){
		return true;
	}
	bool accepted;
	accepted = true;
	vector<bool> accepts;
	int i;
	i=0;
	while(i<queryp->comparisions.size()){
		accepts.push_back(checkWhere(mine1,queryp->comparisions[i],queryp));
		i++;
	}
	
	accepted = accepts[0];
	if(queryp->operators.size()==0){
		return accepted;
	}
	int j,k=0;
	j=0;
	while(j<queryp->operators.size()){
		if(queryp->operators[j]=="or"){
			k=k+1;
			accepted=accepted or accepts[j+1];
		}
		else if(queryp->operators[j]=="and"){
			k=k+1;
			accepted=accepted and accepts[j+1];
		}
		j++;
	}
	
	return accepted;
}

void createCandidate(minedata * mine1 , tdb * mydb , query * queryp){
	vector < int > rowPoint;
	rowPoint=mine1->row_pointer;
	string tablename1;
	string tcol;
	int i;
	i=0;
	while(i<rowPoint.size()){
		tablename1=mine1->table_order[i];
		int j,k;
		j=0;
		k=0;
		while(j<mine1->columns[i].size()){
			if(k==0){
				tcol=tablename1+"."+mine1->columns[i][j];
				k=0;
			}
			mine1->candidate[mine1->col_to_index[tcol]]=mydb->data[tablename1][mine1->columns[i][j]][mine1->row_pointer[i]];
			j++;
		}
		i++;
		
	}
	if(acceptOnWhere(mine1,queryp)){
		join_table.push_back(mine1->candidate);
	}
}

minedata * carryOver(minedata * mine1 , tdb * mydb , query * queryp){
	int first,b;
	first=0;
	b=0;
	int backed ;
	backed = (mine1->total_tables)-1;
	while(mine1->row_pointer[first]>=mine1->table_sizes[first]){
		if(b==0){
			if(first == backed){
				break;
			}
			else{
				mine1->row_pointer[first+1]=(mine1->row_pointer[first+1])+1;
				mine1->row_pointer[first]=0;
				first=first+1;
			}
		}
	}
	if(mine1->row_pointer[backed]<mine1->table_sizes[backed]){
		createCandidate(mine1,mydb,queryp);
	}
	return mine1;
}

void mined(minedata * mine1 , tdb * mydb , query * queryp){
	int backed;
	int b=0;
	backed= (mine1->total_tables)-1;
	while(1){
		if(b==0){
			mine1=carryOver(mine1,mydb,queryp);
			mine1->row_pointer[0]=mine1->row_pointer[0]+1;
			if(mine1->row_pointer[backed]>=mine1->table_sizes[backed]){
				break;
			}
		}
	}
}

void praiseDistinct(minedata * mine1,query * queryp){
	int i;
	bool flag;
	flag=false;
	i=0;
	while(i<queryp->agg.size()){
		if(queryp->agg[i]=="distinct"){
			flag=true;
			break;
		}
		i++;
	}
	
	if(!flag){
		return;
	}
	string colname = queryp->columns[i];
	set< int > discol;
	int columnIndex;
	columnIndex= mine1->col_to_index[colname];
	int j;
	j=0;
	while(j<join_table.size()){
		if(discol.find(join_table[j][columnIndex])!=discol.end()){
			join_table.erase(join_table.begin()+j);
			j--;
			
		}
		else if(discol.find(join_table[j][columnIndex])==discol.end()){
			discol.insert(join_table[j][columnIndex]);
		}	
		j++;
	}
	
	return;
}

void selectFromDb(minedata * mymine,query * query_parsed)
{
	int i,j;
	bool flag=0;
	i=0;
	while(i<query_parsed->columns.size()){
		if(i!=0)
		{
			if(flag==1)
			{	
				flag=0;
				string appended;
				cout<<",";
			}
		}
		if(to_remove.find(query_parsed->columns[i])==to_remove.end())
		{
			cout<<query_parsed->columns[i];
			stringstream appende;
			flag=1;
		}
		i++;
	}	
	cout<<endl;
	i=0;
	while(i<join_table.size()){
		j=0;
		while(j<query_parsed->columns.size())
		{
			if(j!=0 ){
				if (flag==1)
				{
					flag=0;
					string appended1;
					cout<<",";
				}	
			}
			if(to_remove.find(query_parsed->columns[j])==to_remove.end())
			{
				cout<<join_table[i][mymine->col_to_index[query_parsed->columns[j]]];
				stringstream appende;
				flag=1;
			}
			j++;
		}
		cout<<endl;
		i++;
	}
	
}



int main(int argc , char * argv[]){
	if(argc==2){
		int a,r,r1,r2;
		a=0;
		r=0;
		r1=0;
		r2=0;
		if(a==0){
			join_table.clear();
			to_remove.clear();
			if(r==0){
				tdb * udb = rmdata();
				udb = popdb(udb);
				query * queryp=parseit(argv[1]);
				queryp = verifyquery(udb,queryp);
				//print_query(queryp);
				//print_db(udb);
				if(!error){
					if(r2==0){
						minedata * mine = popmine(queryp,udb);
						int r3=0;
						string agg_func=queryp->agg[r3];
						if(sqlFunctions(queryp->agg[r3],udb,queryp)){
							return 0;
						}
						mined(mine,udb,queryp);
						praiseDistinct(mine,queryp);
						selectFromDb(mine,queryp);
					}
				}
				else if(error){
					cout << "Error in Query Processing" << endl; 
				}
			}
		}
	}
	else if(argc!=2)
	{
		cout << "please Enter Input Query\n";
		return 0;
	}
	 
return 0;	
}
