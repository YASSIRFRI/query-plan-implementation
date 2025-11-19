#include <functional>
#include <vector>
#include <iostream>

using namespace std;








class Row {


    //hardocoded 3 ints for the moment
    public:
        int a;
        int b;
        int c;
};





class DataFrame {



    public:
        vector<Row> rows;
        int size(){ 
            return rows.size(); 
        }
};




DataFrame apply_filter(const DataFrame &input, function<bool(const Row&)> condition)
{
    DataFrame result;
    for (const Row &r : input.rows)
        if (condition(r))
            result.rows.push_back(r);
    return result;
}





class Operator{

    public:
        virtual DataFrame* execute() = 0;
};



class Join : public Operator {

    Operator * tobuild;
    Operator * toask;

    public:

        Join(Operator* b, Operator* a) {
            //need to decide beforehand which is what? depneding on size, assume smaller relation is the first
            //argument
            tobuild = b;
            toask   = a;
        }

        DataFrame * execute() override {

            static vector<Row> build_rows;
            static bool built = false;

            if (!built) {
                DataFrame* part;
                while ((part = tobuild->execute())) {
                    for (Row &r : part->rows)
                        build_rows.push_back(r);
                }
                built = true;
            }

            DataFrame* chunk = toask->execute();

            //iterator done
            if(!chunk) return nullptr;

            DataFrame* out = new DataFrame();
            for (Row &p : chunk->rows) {
                for (Row &b : build_rows) {
                    if (p.a == b.a)
                        out->rows.push_back(p);
                }
            }
            return out;
        }
};

class Filter : public Operator {

    Operator* child;
    function<bool(const Row&)> condition;

    public:

        Filter(Operator* c, function<bool(const Row&)> p){
            this->child=c;
            this->condition=p;
        }

        DataFrame* execute() override {
            DataFrame* result;


            while ((result = child->execute())) {
                DataFrame filtered = apply_filter(*result, condition);
                if (filtered.size() > 0)
                    return new DataFrame(filtered);
            }
            return nullptr;
        }
};

class Scanner : public Operator {

    //buffer of
    vector<Row> data;

    //index variable will keep track of where we currenctly are
    int index = 0;

    //how many tuples to push each time
    int chunck_size = 100;

public:
    Scanner(vector<Row> d) : data(std::move(d)) {}

    DataFrame* execute() override {
        //done reading
        if (index >= data.size()) return nullptr;
        DataFrame* df = new DataFrame();
        int end = min(index + chunck_size, (int)data.size());
        for (int i=index; i< end; i++){
            df->rows.push_back(data[i]);
        }

        index = end;
        return df;
    }
};



int main(){

    vector<Row> first{{1,2,3}, {5,6,7}, {8,9,10}};
    vector<Row> second{{1,2,2}, {3,3,5}};



    Operator* first_scanner  = new Scanner(first);
    Operator* second_scanner = new Scanner(second);

    //custom filter condition
    auto lambda=[](const Row& r){
         return r.a == 1; 
    };


    Operator* filter_test = new Filter(second_scanner, lambda);
    Operator* join_test = new Join(first_scanner, filter_test);
    DataFrame* df;


    while((df = join_test->execute())) {
        for (Row &r : df->rows)
            cout<<r.a<<" "<<r.b<<" "<<r.c<<"\n";
    }
    return 0;
}
