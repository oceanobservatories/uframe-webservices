#! /usr/bin/env python

from uframe import UFrame, get_arrays

def main(args):
    '''
    Retrieve the list of available OOI arrays on the uFrame instance.
    '''
   
    if args.base_url:
        uframe_base = UFrame(base_url=args.base_url)
    else:
        uframe_base = UFrame()

    arrays = get_arrays(uframe_base=uframe_base)
    
    if not arrays:
        print 'No arrays found for uFrame instance: {:s}'.format(uframe_base.url)
        return
        
    print 'Available arrays:'
    for array in arrays:
        print array
        
    
if __name__ == '__main__':
    
    import argparse
    
    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('-b', '--baseurl', dest='base_url', help='Specify and alternate uFrame server url')
    
    args = arg_parser.parse_args()
    
    main(args)
