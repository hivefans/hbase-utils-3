
def hexByteDump(data, len = data.length)
   for ix in 0...len
          printf(" %02x", data[ix]);
                 printf("\n") if (ix & 0x1F) == 0x1F
                    end
                       printf("\n")
                       end
print (310891209228288 >> 32).to_s + "\n"
print (310891209228288  & 0x00000000ffff0000) >> 16

#'132'.pack('H')

#def hexlify(msg)
#        msg.split("").collect { |c| print c c[0].to_s(16) }.join
#            end
print [132, 10, 2].pack('L>CC').split("")
#print ['000000840a02'].pack('H')
