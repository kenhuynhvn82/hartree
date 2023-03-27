import apache_beam as beam

all_group_columns = ['legal_entity', 'counter_party', 'tier']

# use these classes if newline delimiters are 'n' and 'rn'
# https://beam.apache.org/releases/pydoc/2.6.0/apache_beam.io.textio.html
""" class Dataset1(beam.DoFn):
    def process(self, element):
        invoice_id,legal_entity,counter_party,rating,status,value = element.split(',')
        return [{
            'invoice_id': int(invoice_id),
            'legal_entity': legal_entity,
            'counter_party': counter_party,
            'rating': int(rating),
            'status': status,
            'value': int(value)
        }]
        
class Dataset2(beam.DoFn):
    def process(self, element):
        counter_party,tier = element.split(',')
        return [{
            'counter_party': counter_party,
            'tier': int(tier)
        }] """
        
class Dataset1(beam.DoFn):
    def process(self, element):
        lines = element.split('\r')
        for line in lines[1:]:
            invoice_id,legal_entity,counter_party,rating,status,value = line.split(',')
            yield {
                'invoice_id': int(invoice_id),
                'legal_entity': legal_entity,
                'counter_party': counter_party,
                'rating': int(rating),
                'status': status,
                'value': int(value)
            }
        
class Dataset2(beam.DoFn):
    def process(self, element):
        lines = element.split('\r')
        for line in lines[1:]:
            counter_party,tier = line.split(',')
            yield {
                'counter_party': counter_party,
                'tier': int(tier)
            }
        
class JoinedDataset(beam.DoFn):
    def process(self, element):
        key_col, group = element
        for el in group[0]:
            el.update({k: v for k, v in group[1][0].items() if k != 'counter_party'}) 
            yield el
    
def to_beam_row(row):
    return beam.Row(**row)

def to_csv_line(row):
    line = ''
    for col in all_group_columns:
        line += str(getattr(row, col)) if hasattr(row, col) else 'Total'
        line += ','
    return line + str(row.max_rating) + ',' + str(row.sum_status_ARAP) + ',' + str(row.sum_status_ACCR)

def merge_two_datasets_by_beam(input_file1: str, input_file2: str, output_file: str):
    with beam.Pipeline() as p:
    
        dataset1 = (
            p 
            #| 'Read dataset1 file (if newline delimiters are 'n' and 'rn')' >> beam.io.ReadFromText(input_file1, skip_header_lines=1) 
            | 'Read dataset1 file' >> beam.io.ReadFromText(input_file1) 
            | 'Parse dataset1' >> beam.ParDo(Dataset1())
            | 'Group dataset1 by counter_party' >> beam.Map(lambda row: (row['counter_party'], row))
        )
        
        dataset2 = (
            p 
            #| 'Read dataset2 file (if newline delimiters are 'n' and 'rn')' >> beam.io.ReadFromText(input_file2, skip_header_lines=1) 
            | 'Read dataset2 file' >> beam.io.ReadFromText(input_file2) 
            | 'Parse dataset2' >> beam.ParDo(Dataset2())
            | 'Group dataset2 by counter_party' >> beam.Map(lambda row: (row['counter_party'], row))
        )
        
        groupByAll = (
            [dataset1, dataset2] 
            | beam.CoGroupByKey() 
            | beam.ParDo(JoinedDataset()) 
            | beam.Map(to_beam_row) 
            | beam.GroupBy(*all_group_columns)
                .aggregate_field('rating', max, 'max_rating')
                .aggregate_field(lambda x: x.value if x.status == "ARAP" else 0, sum, 'sum_status_ARAP')
                .aggregate_field(lambda x: x.value if x.status == "ACCR" else 0, sum, 'sum_status_ACCR')
        )
        
        final = [groupByAll]
        for attr in [['legal_entity'], ['legal_entity', 'counter_party'], ['counter_party'], ['tier']]:
            groupByAttr = (
                groupByAll
                | 'Group by ' + '.'.join(attr) >> beam.GroupBy(*attr)
                    .aggregate_field('max_rating', max, 'max_rating')
                    .aggregate_field('sum_status_ARAP', sum, 'sum_status_ARAP')
                    .aggregate_field('sum_status_ACCR', sum, 'sum_status_ACCR')
            )
            final.append(groupByAttr)
        
        (
            final
            | beam.Flatten()
            | beam.Map(to_csv_line)
            | beam.io.WriteToText(output_file, file_name_suffix='.csv', header='legal_entity,counter_party,tier,max_rating,sum_status_ARAP,sum_status_ACCR')
        )
        