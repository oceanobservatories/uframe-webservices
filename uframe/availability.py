import sys
import csv
import os
from collections import OrderedDict
# from ~/code/pylib
from uframe import *

def test_product_availability(test_csv, uframe=None, resultsdir=None, out_csv=None):
    
    out_csv = None
    
    # csv file exists
    if not os.path.exists(test_csv):
        sys.stderr.write('Invalid test file specified: {:s}\n'.format(test_csv))
        sys.stderr.flush()
        return out_csv
        
    if not uframe:
        uframe = UFrame()
        
    # Open up test_csv for reading
    try:
        fid = open(csv_file, 'rU')
    except IOError as e:
        sys.stderr.write('{:s}: {:s}\n'.format(e.message, e.filename))
        sys.stderr.flush()
        return out_csv
        
    # Create the csv reader instance
    try:
        c = csv.reader(fid)
    except csv.Error as e:
        sys.stderr.write('{:s}: {:s}' % (test_csv, e))
        sys.stderr.flush()
        fid.close()
        return out_csv
        
    # Open the output file to write the results
    try:
        if not out_csv:
            (out_path, out_file) = os.path.split(csv_file)
            (f_name, ext) = os.path.splitext(out_file)
            csv_name = '{:s}-test_results{:s}'.format(f_name, ext)
            out_csv = os.path.join(out_path, csv_name)
        # Open out_csv to write the results
        out_fid = open(out_csv, 'w')
    except IOError as e:
        sys.stderr.write('{:s}: {:s}\n'.format(e.message, e.filename))
        sys.stderr.flush()
        fid.close()
        return None
        
    # Create the csv writer instance
    try:
        out_writer = csv.writer(out_fid)
    except csv.Error as e:
        sys.stderr.write('{:s}: {:s}' % (test_csv, e))
        sys.stderr.flush()
        fid.close()
        out_fid.close()
        return None
        
    # test_csv column indices
    refdes = 1
    r_stream = 3
    t_stream = 4
    r_param = 5
    t_param = 6
    
    # dict for holding metadata urls
    all_metadata = {}
    
    # First line of test_csv contains column headers
    headers = c.next()
    # Add test result columns
    all_tests = OrderedDict()
    all_tests['DataStreamR Available'] = 0
    all_tests['ParameterID_R Available'] = 0
    all_tests['ParameterID_R in Stream'] = 0
    all_tests['DataStreamT Available'] = 0
    all_tests['ParameterID_T Available'] = 0
    all_tests['ParameterID_T in Stream'] = 0
    all_tests['UFrame DataStreamR'] = ''
    all_tests['UFrame DataStreamT'] = ''
    all_tests['UFrame Metadata URL'] = ''
    
    for h in all_tests.keys():
        headers.append(h)
    
    # Write the output headers
    out_writer.writerow(headers) 
    
    for row in c:
    
        # Add the test result cells               
        for (k,v) in all_tests.items():
            row.append(v)
            
        # Break up the reference designator to create the metadata request url
        ref_tokens = row[refdes].split('-')
        if len(ref_tokens) != 4:
            sys.stderr.write('Invalid reference designator: {:s}\n'.format(row[refdes]))
            sys.stderr.flush()
            # Write the results to the output file
            out_writer.writerow(row)
            continue
        
        if row[refdes] in all_metadata.keys() and not all_metadata[row[refdes]]:
            continue
        elif row[refdes] in all_metadata.keys():
            meta = all_metadata[row[refdes]]
        else:    
            # Attempt to fetch the metadata
            sys.stdout.write('{:s}: Fetching metadata\n'.format(row[refdes]))
            sys.stdout.flush()
            
            meta = get_sensor_metadata(ref_tokens[0], ref_tokens[1], '{:s}-{:s}'.format(ref_tokens[2], ref_tokens[3]))
            if not meta:
                all_metadata[row[refdes]] = None
                # Write the results to the output file
                out_writer.writerow(row)
                continue
            
            # Cache the metadata record so we don't have to fetch it each time for 
            # the same reference designator
            all_metadata[row[refdes]] = meta   
        
        # Create the metadata url
        url = uframe.url + '/{:s}/{:s}/{:s}/metadata'.format(
            ref_tokens[0],
            ref_tokens[1],
            '{:s}-{:s}'.format(ref_tokens[2], ref_tokens[3])
        ) 
        i = headers.index('UFrame Metadata URL')
        row[i] = url
        
        # Create the list of available data streams and parameters
        streams = [m['stream'] for m in meta['times']]
        parameters = [m['particleKey'] for m in meta['parameters']]
        
        # RECOVERED
        if row[r_stream]:
            
            # 1. Is the recovered data stream (r_stream) available?
            if row[r_stream] in streams:
                i = headers.index('DataStreamR Available')
                row[i] = 1
                
                # 2. Is the recovered parameter (r_param) available?
                if row[r_param]:
                    if row[r_param] in parameters:
                        i = headers.index('ParameterID_R Available')
                        row[i] = 1
                        
                        # 3. Is the recovered parameter (r_param) identified 
                        # with the stream (r_stream)?
                        i = parameters.index(row[r_param])
                        if meta['parameters'][i]['stream'] == row[r_stream]:
                            i = headers.index('ParameterID_R in Stream')
                            row[i] = 1
                        
                        # See if the parameter (particleKey) is associated with any stream
                        stream = get_parameter_stream(meta, row[r_param], 'recovered')
                        
                        if stream:
                            i = headers.index('UFrame DataStreamR')  
                            row[i] = stream
    
        elif row[r_param] and row[r_param] in parameters:
            # If no stream was specified for this test case (row), see if the 
            # parameter (particleKey) is associated with any stream
            stream = get_parameter_stream(meta, row[r_param], 'recovered')
                                
            if stream:
                i = headers.index('UFrame DataStreamR')  
                row[i] = meta['times'][stream_i]['stream']
                
        else:
            sys.stderr.write('{:s}: No recovered stream specified\n'.format(row[refdes]))
            sys.stderr.flush()
            
        # TELEMETERED
        if row[t_stream]:
            
            # 4. Is the telemetered data stream (t_stream) available?
            if row[t_stream] in streams:
                i = headers.index('DataStreamT Available')
                row[i] = 1
                
                # 5. Is the telemetered parameter (t_param) available?
                if row[t_param]:
                    if row[t_param] in parameters:
                        i = headers.index('ParameterID_T Available')
                        row[i] = 1
                        
                        # 6. Is the telemetered parameter (t_param) identifid with the stream (t_stream)
                        i = parameters.index(row[t_param])
                        if meta['parameters'][i]['stream'] == row[t_stream]:
                            i = headers.index('ParameterID_T in Stream')
                            row[i] = 1
                        
                        # See if the parameter (particleKey) is associated with any telemetered stream
                        stream = get_parameter_stream(meta, row[r_param], 'telemetered')
                        
                        if stream:
                            i = headers.index('UFrame DataStreamT')  
                            row[i] = stream
                              
        elif row[t_param] and row[t_param] in parameters:
            # If no stream was specified for this test case (row), see if the 
            # parameter (particleKey) is associated with any stream
            stream = get_parameter_stream(meta, row[r_param], 'recovered')
                                
            if stream:
                i = headers.index('UFrame DataStreamT')  
                row[i] = meta['times'][stream_i]['stream']             
        else:
            sys.stderr.write('{:s}: No telemetered stream specified\n'.format(row[refdes]))
            sys.stderr.flush()
            
        # Write the results to the output file
        out_writer.writerow(row)
        
    fid.close()
    out_fid.close()
    
    return out_csv
    
def get_parameter_stream(metadata, parameter, method=None):
    
    parameters = [m['particleKey'] for m in meta['parameters']]
    
    if not parameters:
        sys.stderr.write('No parameters found in metadata record\n')
        sys.stderr.flush()
        return []
    elif parameter in parameters:
        sys.stderr.write('Parameter not found in metadata record: {:s}\n'.format(parameter))
        sys.stderr.flush()
        return []
        
    particle_streams = [x['stream'] for i,x in enumerate(meta['parameters']) if x['particleKey'] == row[t_param]]
    stream_i = -1
    streams = []
    for stream in particle_streams:
        if stream_i > -1:
            break
        for t in range(len(meta['times'])):
            if method:
                if meta['times'][t]['stream'] == stream and meta['times'][t]['method'] == method:
                    streams.append(meta['times'][t])
            else:
                if meta['times'][t]['stream'] == stream:
                    streams.append(meta['times'][t])
        
        return streams