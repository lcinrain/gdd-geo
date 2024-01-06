
def get_HMDistance(ip1,ip2):
    '''
    nibble similarity between IPv6 addresses. if EUI-64, 22 nibbles are compared.
    '''
    ipv61 = ipv62hexstr(ip1)
    ipv62 = ipv62hexstr(ip2)
    distance = 0
    end = 22 if 'fffe' in ipv61 or 'fffe' in ipv62 else 32
    for i in range(0,end):
        if ipv61[i] == ipv62[i]: distance+=1
    return distance

def compressed(hextets):
    best_doublecolon_start = -1
    best_doublecolon_len = 0
    doublecolon_start = -1
    doublecolon_len = 0
    for index, hextet in enumerate(hextets):
        if hextet == '0':
            doublecolon_len += 1
            if doublecolon_start == -1:
                # Start of a sequence of zeros.
                doublecolon_start = index
            if doublecolon_len > best_doublecolon_len:
                # This is the longest sequence of zeros so far.
                best_doublecolon_len = doublecolon_len
                best_doublecolon_start = doublecolon_start
        else:
            doublecolon_len = 0
            doublecolon_start = -1

    if best_doublecolon_len > 1:
        best_doublecolon_end = (best_doublecolon_start +
                                best_doublecolon_len)
        # For zeros at the end of the address.
        if best_doublecolon_end == len(hextets):
            hextets += ['']
        hextets[best_doublecolon_start:best_doublecolon_end] = ['']
        # For zeros at the beginning of the address.
        if best_doublecolon_start == 0:
            hextets = [''] + hextets
    return hextets

def hexstr2ipv6(hex_str):
    '''
    Parameters:
        - hex_str: str. full IPv6 str withOUT ':' among nybbles, e.g. 20010db8000000000000000000000000
    Returns:
        compressed IPv6 addresses: str.  e.g.  2001:db8:: 
    '''
    hextets = ['%x' % int(hex_str[x:x+4], 16) for x in range(0, 32, 4)]
    a= compressed(hextets)
    return ':'.join(a)


def ipv62hexstr(ip:str):
    '''
2001:db8:: to 20010db8000000000000000000000000
'''
    #return ipaddress.IPv6Address(ip).exploded.replace(':','')
    return ipv6explod(ip)

def ipseg2str(ipseglist:list):
    strlist = []
    for ipseg in ipseglist:
        hexstr = (4-len(ipseg))*'0'+ipseg
        strlist.append(hexstr)
    return ''.join(strlist)


def compressedipv6exploded(parts_list:list):
    '''
    explode ipv6 str with '::' in it. parts_list = ipv6.split('::')
    '''
    part1, part2 = parts_list
    part1_list = part1.split(':') if part1 else []
    part2_list = part2.split(':') if part2 else []
    middle0str = '0000'*(8-len(part1_list)-len(part2_list))
    ipv6_exploded = ipseg2str(part1_list)+middle0str+ipseg2str(part2_list)
    return ipv6_exploded

def ipv6explod(ipv6:str):
    ipv6exploded = ''
    if '::' in ipv6:
        ipv6exploded = compressedipv6exploded(ipv6.split('::'))
    else:
        ipv6exploded = ipseg2str(ipv6.split(':'))
    return ipv6exploded



if __name__ == '__main__':
    
    ipv61 = '2001:dc00::'
    ipv62 = '2001:dd00::'
    print(hexstr2ipv6(ipv62hexstr(ipv61))==ipv61)
    print(get_HMDistance(ipv61,ipv62))
    
    