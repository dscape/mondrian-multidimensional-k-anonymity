
# ==================================================================================
# anonymization: group.rb
# ==================================================================================
ENVIRONMENT = 'release' #'release'

require 'set'
require 'rubygems'
require 'ruby-debug' if ENVIRONMENT == 'debug'

# ==================================================================================
# class group
#
# usage:
#  require 'group'
#
#  g = Group.new <quasi_ids>, <filename>
#  g.anonymize <k>
#
# example:
#
# lefevre.db
#
#     0             2   < -- quasi_ids
#
#   |age|  sex  | zipc | disease      |
#---+---+-------+------+--------------+--
# 0 | 25  Male    53711 Flu           |
# 1 | 25  Female  53712 Hepatitis     |
# 2 | 26  Male    53711 Bronchitis    |
# 3 | 27  Male    53710 Broken_Arm    |
# 4 | 27  Female  53712 AIDS          |
# 5 | 28  Male    53711 Hang_Nail     |
#---+---+-------+------+--------------+--
#
# irb
#  >> require 'group'
#  >> g = Group.new [0,2], 'lefevre.db'
#  >> g.anonymize 2, 'degen'
# ==================================================================================
class Group
  # create a setter method for @tuples, @filename
  # so that g.tuples = x works
  attr_writer :tuples, :filename

  @@debug = { 'best_attribute' => ENVIRONMENT == 'debug',
              'intersection'   => ENVIRONMENT == 'debug',
              'split'          => ENVIRONMENT == 'debug',
              'ordering'       => ENVIRONMENT == 'debug',
              'vars'           => ENVIRONMENT == 'debug',
              'args'           => ENVIRONMENT == 'debug'
           }
  # ================================================================================
  # to create a new group with Group.new
  # ================================================================================
  # needs to remove the full_ids from the read.
  def initialize(quasi_ids, filename, depth=0, available_ids=nil)
    # if no valid attributes are given quasi are used
    available_ids = quasi_ids if available_ids.nil?

    # initialize the instance vars
    @tuples = []
    @quasi_ids = quasi_ids
    @available_ids = available_ids
    @depth = depth

    # serves as wilcard so that no file is read on recursion
    filename == '*wc' ? @filename = nil : @filename = filename

    if @@debug['args'] and @depth == 0
      debug_puts "args : file => #{@filename}"
      debug_puts "args : k => #{@k}"
      debug_puts "args : quasi_ids => #{@quasi_ids.to_s}"
    end

    # run the read and backup procedures
    read
  end

  # ================================================================================
  # anonymization
  # ================================================================================
  def anonymize(k, heuristic='degen', partial_order=[])

    if @@debug['vars']
      #debug_puts "dvars : @tuples #{@tuples}"
      debug_puts "dvars : @available_ids #{@available_ids},"
      debug_puts "dvars : @depth #{@depth}"
    end

    # stop case
    if isnt_splittable? k
      debug_puts "dsplit: no split available for k-level #{k} with size" +
                 " #{@tuples.size}" if @@debug['split']

      # sort and generalize remaining attributes
      @available_ids.each do |attribute|
        sort attribute
        generalize attribute
      end

      # exit
      return
    end

    # where and in what attribute should we split
    # these functions have a heavy effect on the usefulness of the information
    # for the k-anonymity table
    split_attribute  = find_split_attribute @available_ids, heuristic, partial_order
    split_pos        = find_split_position split_attribute

    # create the groups for the
    # recursion
    group1 = Group.new @quasi_ids, '*wc', @depth + 1, @available_ids.clone
    group2 = Group.new @quasi_ids, '*wc', @depth + 1, @available_ids.clone

    # split at the given position
    split split_pos, group1, group2

    if split_groups_satisfy_k_anonymity?(k,group1,group2)

      debug_puts "dsplit: no more split available with attribute" +
          " #{split_attribute} (g1: #{group1.size}, g2: #{group2.size})" if @@debug['split']

      # generalize by split_attribute and then remove it from the available
      # attributes array
      generalize split_attribute
      @available_ids.delete split_attribute

      # anonymize remaining available attributes
      anonymize k, heuristic, partial_order

    else # splitting successful
      debug_puts "dsplit: splitting on attribute #{split_attribute} at" +
                 " position #{split_pos} of #{@tuples.size}" if @@debug['split']

      # assign the two groups to this instance
      @group1 = group1
      @group2 = group2

      group1.anonymize k, heuristic, partial_order
      group2.anonymize k, heuristic, partial_order

      #@tuples = []
    end
  end

  # ================================================================================
  # io and backup related
  # ================================================================================
  # read @tuples from @filename
  def read
    unless @filename.nil?
      f = File.open @filename
      f.each_line do |line|
        @tuples < < line.rstrip.split("\t\t")
      end
      f.close
    end
  end

  # reset the class to reuse
  def reset
    @available_ids  = @originally_available_ids
    @tuples = []
    read
  end

  # ================================================================================
  # overrides
  # ================================================================================
  # number of tuples
  def size
    @tuples.size
  end

  # ================================================================================
  # aux
  # ================================================================================
  # to_s

  def to_s

    str = ""

    unless @tuples.empty?
      @tuples.each do |line|
        @tuples[0].size.times { |i| str << line[i].to_s + "\t\t"}
        str << "\n"
      end
    end

    str
  end

  # shows a yaml representation of internal object
  def to_y
    require 'yaml'
    y self
  end

  private

  def debug_puts(message)
    ident=''
    @depth.times {|i| ident+="  "}
    puts ident + message
  end

  # ================================================================================
  # aux for anonymization
  # ================================================================================
  # finds the attribute with the largest range. According to LeFevre this is a good
  # heuristic to find the attribute on
  def find_split_attribute(attributes_list, heuristic, partial_order)

    debug_puts "dorder: choosing from" +
               " #{attributes_list.to_s}" if @@debug['ordering']

    best_attrib = -1
    best_attrib_count = 0.0

    attributes_list = find_minimal_elements partial_order, attributes_list

    debug_puts "dorder: minimal list is" +
               " #{attributes_list.to_s}" if @@debug['ordering']

    attributes_list.each do |attribute|
      values = @tuples.map{|t| t[attribute]}.to_set

      # degen heuristic: split on the attribute that had more degeneracy
      if heuristic == 'degen'
        if values.size < best_attrib_count or best_attrib == -1
          best_attrib = attribute
          best_attrib_count = @tuples.size.to_f / values.size.to_f
        end
      elsif heuristic == 'single'
        if values.size < best_attrib_count or best_attrib == -1
          best_attrib = attribute
          best_attrib_count = values.size
        end
      else #default
        if values.size > best_attrib_count
          best_attrib = attribute
          best_attrib_count = values.size
        end
      end
    end

    debug_puts "dbest : best atribute is #{best_attrib} with" +
               " count #{best_attrib_count}" if @@debug['best_attribute']

    return best_attrib
  end

  #  returns the position of the leftmost or rightmost median element.
  #  used to split in lhs and rhs
  def find_split_position(attribute_id)
    sort attribute_id

    median_pos = @tuples.size / 2
    median = @tuples[median_pos][attribute_id]

    split_pos_high = median_pos
    split_pos_low  = median_pos

    # split point correspond to highest index that has median value
    split_pos_high += 1 while (@tuples.size >= split_pos_high + 2) and
                              (@tuples[split_pos_high + 1][attribute_id] == median)

    high_smaller_group_size =
            [split_pos_high + 1, @tuples.size - split_pos_high - 1].min

    # split point correspond to lowest index that has median value
    split_pos_low -= 1 while (split_pos_low > 1) and
                              (@tuples[split_pos_low - 1][attribute_id] == median)

    low_smaller_group_size =
            [split_pos_low, @tuples.size - split_pos_low].min

    # choose the one with the largest group
    if high_smaller_group_size > low_smaller_group_size
      split_pos = split_pos_high
    else
      split_pos = split_pos_low - 1
    end

    return split_pos
  end

  # finds minimal elements from the list of the given attribute list according to
  # partial order specified in partial_order. partial_order contains all complete chains.
  def find_minimal_elements(partial_order, possible_elements)

    if partial_order.empty?
      debug_puts "dorder: no ordering specified" if @@debug['ordering']

      return possible_elements
    end

    # choose all possible_elements that arent in partial_order
    # those are minimal
    minimal_list = possible_elements.select { |element| !partial_order.flatten.member?(element) }

    # haskell goodies ^^
    # restrict partial_order to values in possible_elements
    restricted_partial_order = partial_order.map { |l| l.select { |element| possible_elements.member?(element) } }

    if @@debug['ordering']
      debug_puts "dorder: possible_elements list is" +
                 " #{possible_elements.to_s}"
      debug_puts "dorder: partial_order list is" +
                 " #{partial_order.to_s}"
      debug_puts "dorder: restricted_partial_order is" +
                 " #{restricted_partial_order.to_s}"
    end

   non_zero_chains = restricted_partial_order.select { |chain| not chain.empty? }

   non_zero_chains.each do |c|
     candidate = c[0]

     minimal = !restricted_partial_order.any? do |chain|
        chain.member?(candidate) and chain[0] != candidate
     end

     if minimal and not minimal_list.member?(candidate)
       minimal_list << candidate
     end
   end

   return minimal_list
  end

  # replaces attribute value with generalization that cover all tuples.
  # Expects tuples to be sorted by attribute.
  def generalize(attribute)
    min_val = @tuples[0][attribute]
    max_val = @tuples[-1][attribute]

    unless min_val == max_val
      @tuples.each do |t|
        t[attribute] = [min_val, max_val]
      end
    end

  end

  def split(split_pos, group1, group2)
    group1.tuples = @tuples[0..split_pos]
    group2.tuples = @tuples[split_pos+1..@tuples.size]
  end

  def sort(attribute)
    @tuples = @tuples.sort_by { |t| t[attribute] }
  end

  # ================================================================================
  # verbose conditions
  # ================================================================================
  def isnt_splittable?(k)
    k < 2 or group_cant_be_split_for_level?(k) or no_split_attributes_are_available?
  end

  def group_cant_be_split_for_level?(k)
    @tuples.size < 2*k
  end

  def no_split_attributes_are_available?
    @available_ids.empty?
  end

  def split_groups_satisfy_k_anonymity?(k,group1,group2)
    group1.size < k or group2.size < k
  end
end

# hack on array to display lists correctly
class Array
  def to_s
    "[" + self.join(',') + "]"
  end
end
