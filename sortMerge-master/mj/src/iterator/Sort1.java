package iterator;

import heap.*;
import global.*;
import diskmgr.*;
import bufmgr.*;
import index.*;
import java.io.*;
import iterator.TupleUtils;

/*==========================================================================*/
/**
 * Sort-merge join.
 * Call the two relations being joined R (outer) and S (inner).  This is an
 * implementation of the naive sort-merge join algorithm.  The external
 * sorting utility is used to generate runs.  Then the iterator interface
 * is called to fetch successive tuples for the final merge with joining.
 */

public class Sort1 extends Iterator implements GlobalConst {

    /**
     *constructor,initialization
     *@param in1[]         Array containing field types of R
     *@param len_in1       # of columns in R
     *@param s1_sizes      Shows the length of the string fields in R.
     *@param in2[]         Array containing field types of S
     *@param len_in2       # of columns in S
     *@param s2_sizes      Shows the length of the string fields in S
     *@param sortFld1Len   The length of sorted field in R
     *@param sortFld2Len   The length of sorted field in S
     *@param join_col_in1  The col of R to be joined with S
     *@param join_col_in2  The col of S to be joined with R
     *@param amt_of_mem    Amount of memory available, in pages
     *@param am1           Access method for left input to join
     *@param am2           Access method for right input to join
     *@param in1_sorted    Is am1 sorted?
     *@param in2_sorted    Is am2 sorted?
     *@param order         The order of the tuple: assending or desecnding?
     *@param outFilter[]   Ptr to the output filter
     *@param proj_list     Shows what input fields go where in the output tuple
     *@param n_out_flds    Number of outer relation fileds
     *@exception JoinNewFailed               Allocate failed
     *@exception JoinLowMemory               Memory not enough
     *@exception SortException               Exception from sorting
     *@exception TupleUtilsException         Exception from using tuple utils
     *@exception JoinsException              Exception reading stream
     *@exception IndexException              Exception...
     *@exception InvalidTupleSizeException   Exception...
     *@exception InvalidTypeException        Exception...
     *@exception PageNotReadException        Exception...
     *@exception PredEvalException           Exception...
     *@exception LowMemException             Exception...
     *@exception UnknowAttrType              Exception...
     *@exception UnknownKeyTypeException     Exception...
     *@exception IOException                 Some I/O fault
     *@exception Exception                   Generic
     */

	Iterator am1s;
	Iterator am2s;
	int join_col_in1;
	int join_col_in2;
	int len_in1;
	int len_in2;
	AttrType in1[];
	AttrType in2[];
	short s1_sizes[];
	short s2_sizes[];
	
	
	//Buffer page size implementation 
	private byte[][] matchSpaceInner;
	private byte[][] matchSpaceOuter;
	
	
	//Implement heap files for both tables
	private Heapfile innerHeap,outerHeap;
	
	
	//Declaring buffer pools variables for both tables 
	private IoBuf dupOuterBuf = new IoBuf();
	private IoBuf dupInnerBuf = new IoBuf();
	
	private Tuple joinTuple;
	private CondExpr outFilter[];
	private FldSpec proj_list[];
	private int n_out_flds;
	
	// Instances of tuples
	private Tuple temp1;
	private Tuple temp2;
	private Tuple tupleInner;
	private Tuple tupleOuter;
	
	
	private AttrType sortFieldType;
	private boolean get_from_inner, get_from_outer;
	
	// Error checking variables for getNext 
	private boolean complete;
	private boolean move_to_next;
	
	private TupleOrder orderSorted;
	
    public Sort1(
        AttrType    in1[], 
        int         len_in1,                        
        short       s1_sizes[],
        AttrType    in2[],                
        int         len_in2,                        
        short       s2_sizes[],

        int         join_col_in1,                
        int         sortFld1Len,
        int         join_col_in2,                
        int         sortFld2Len,

        int         amt_of_mem,               
        Iterator    am1, // outter               
        Iterator    am2, // inner               

        boolean     in1_sorted,                
        boolean     in2_sorted,                
        TupleOrder  order,

        CondExpr    outFilter[],                
        FldSpec     proj_list[],
        int         n_out_flds
        

        
    )
    throws JoinNewFailed ,
        JoinLowMemory,
        SortException,
        TupleUtilsException,
        JoinsException,
        IndexException,
        InvalidTupleSizeException,
        InvalidTypeException,
        PageNotReadException,
        PredEvalException,
        LowMemException,
        UnknowAttrType,
        UnknownKeyTypeException,
        IOException,
        Exception
    {
    	this.join_col_in1 = join_col_in1;
    	this.join_col_in2 = join_col_in2;
    	this.s1_sizes = s1_sizes;
    	this.s2_sizes = s2_sizes;
    	
    	this.outFilter = outFilter;
    	this.orderSorted = order;
    	
    	
    	this.proj_list = proj_list;
    	this.n_out_flds = n_out_flds;
    	this.in1 = in1;
    	this.in2 = in2;
    	
    	
    	this.matchSpaceInner = new byte[1][MINIBASE_PAGESIZE];
    	this.matchSpaceOuter = new byte[1][MINIBASE_PAGESIZE];
    	
    	sortFieldType = in1[join_col_in1];
    	
    	this.get_from_inner = true;
    	this.get_from_outer = true;
    	this.move_to_next = true;
    	this.complete = false;
    	
    
    	
    	//Start initializing I/O buffs
        AttrType[] joinTypes = new AttrType[n_out_flds];
        short[]  ts_size   = null;
        Tuple joinTuple = new Tuple();
        try {
        	ts_size =
        
        		TupleUtils.setup_op_tuple(
        				joinTuple,  // Output: Gets initialized by the call.
        				joinTypes,  // Output: Gets initialized by the call.
        				in1,        // Input: Attr-Type array for outer.
        				len_in1,    // Input: #columns of outer schema
        				in2,        // Input: Attr-Type array for inner.
        				len_in2,    // Input: #columns of inner schema
        				s1_sizes,   // Input: Array w/ sizes of string columns for outer.
        				s2_sizes,   // Input: Array w/ sizes of string columns for inner.
        				proj_list,  // Input: Says what columns to keep for joinTuple.
        				n_out_flds  // Input: #columns in joinTuple's schema.
        				);
        	}
        catch (Exception e) {
        	throw new TupleUtilsException (e, "This exception is already being catched.");
        }
        
        innerHeap = null;
        try {
            innerHeap = new Heapfile(null);
        }
        catch(Exception e) {
            throw new SortException (e, "Creating heapfile for IoBuf use failed.");
        }
        
        //confirming the size - There is a block of code to be implemented - line 123-128
        
        
       // Reassign the iterators to temp variables in order to 
        am1s = am1;
		am2s= am2;
		
		// Initializing sortFld for Outer tables
		sortFieldType = in1[join_col_in1];
		
		// Determine if table 1 is sorted
		if (!in1_sorted) {
			try {
			am1s = new Sort(in1, (short) len_in1, s1_sizes, am1, join_col_in1, order, sortFld1Len, amt_of_mem / 2);
		} catch(Exception e)
        {
			throw new SortException(e, "Sort of table 1 failed.");
		}
		}
		
		// Determine if table 2 is sorted
    	if (!in2_sorted) {
    		try {
    			am2s = new Sort(in2, (short) len_in2, s2_sizes, am2, join_col_in2, order, sortFld1Len, amt_of_mem / 2);
    		}
    		catch (Exception e) {
    			throw new SortException(e, "Sort of table 2 failed.");
    		}
  
		}
    	
    	
    	// Exception checking to ensure no instances of tuples or buffer pool is not initialized 
    
    		  if (dupOuterBuf  == null || dupInnerBuf  == null ||
    		  temp1 == null || temp2==null ||
    		  tupleInner ==  null || tupleOuter ==null)
    		  throw new JoinNewFailed ("Memory not allocated");
    		  
    		  
    	// Set header for each tuple to allocate memory? 
    		  try 
    		  {
    			  temp1.setHdr((short)len_in1, in1, s1_sizes);
    			  tupleInner.setHdr((short)len_in1, in1, s1_sizes);
    			  temp2.setHdr((short)len_in2, in2, s2_sizes);
    			  tupleOuter.setHdr((short)len_in2, in2, s2_sizes);
    		  }catch (Exception e) {
    		  throw new SortException (e,"Failed to initialize header");
    		  }
    		  
    		

    	
    	// At this point you may have to throw new exception for heap being not null
    		  try {
    				innerHeap = new Heapfile(null);
    				outerHeap = new Heapfile(null);
    				
    			      }
    			      catch(Exception e) {
    				throw new SortException (e, "Heap file cannot be created");
    			}
    	
    	



    } // End of SortMerge constructor

/*--------------------------------------------------------------------------*/
    /**
     *Reads a tuple from a stream in a less painful way.
     */
    @SuppressWarnings("unused")
	private boolean readTuple(Tuple tuple, Iterator tupleStream)
        throws JoinsException,
            IndexException,
            UnknowAttrType,
            TupleUtilsException,
            InvalidTupleSizeException,
            InvalidTypeException,
            PageNotReadException,
            PredEvalException,
            SortException,
            LowMemException,
            UnknownKeyTypeException,
            IOException,
            Exception
    {
        Tuple temp = new Tuple();
        temp = tupleStream.get_next();
        if (temp != null) {
            tuple.tupleCopy(temp);
            return true;
        } else {
            return false;
        }
    } // End of readTuple

/*--------------------------------------------------------------------------*/
    /**
     *Return the next joined tuple.
     *@return the joined tuple is returned
     *@exception IOException I/O errors
     *@exception JoinsException some join exception
     *@exception IndexException exception from super class
     *@exception InvalidTupleSizeException invalid tuple size
     *@exception InvalidTypeException tuple type not valid
     *@exception PageNotReadException exception from lower layer
     *@exception TupleUtilsException exception from using tuple utilities
     *@exception PredEvalException exception from PredEval class
     *@exception SortException sort exception
     *@exception LowMemException memory error
     *@exception UnknowAttrType attribute type unknown
     *@exception UnknownKeyTypeException key type unknown
     *@exception Exception other exceptions
     */

    public Tuple get_next() 
        throws IOException,
           JoinsException ,
           IndexException,
           InvalidTupleSizeException,
           InvalidTypeException, 
           PageNotReadException,
           TupleUtilsException, 
           PredEvalException,
           SortException,
           LowMemException,
           UnknowAttrType,
           UnknownKeyTypeException,
           Exception
	{
    	

    	
    	
    	//Personal Code 
    	
    	
    	int order_of_table ;
    	
    	// Checking to see if any table arrived at the end of tuple stream
    	order_of_table = TupleUtils.CompareTupleWithTuple(sortFieldType,tupleInner, join_col_in1, tupleOuter,join_col_in2);
    	while((order_of_table < 0 && orderSorted.tupleOrder == TupleOrder.Ascending) || (order_of_table > 0 && orderSorted.tupleOrder == TupleOrder.Descending))
    	{
    		if ((tupleOuter = am1s.get_next()) == null || (tupleInner = am2s.get_next()) == null) {
    			complete = true;
    			return null;
    		}
    		else if(order_of_table != 0) {
    			move_to_next = true;
    			continue;
    		}
    		
    		
    		order_of_table = TupleUtils.CompareTupleWithTuple(sortFieldType,tupleInner, join_col_in1, tupleOuter,join_col_in2);
    	
    	}
    	
    	//Putting the values of outer and inner tuples into temporary tuples 
    	temp1.tupleCopy(tupleOuter);
    	temp2.tupleCopy(tupleInner);
    	
    	
    	dupOuterBuf.init(matchSpaceOuter, 1, len_in1, outerHeap);
    	dupInnerBuf.init(matchSpaceInner, 1, len_in2, innerHeap);
    	
    	//
    	while ((TupleUtils.CompareTupleWithTuple(sortFieldType, tupleInner, join_col_in1, tupleOuter, join_col_in2) == 0) || 
    	(TupleUtils.CompareTupleWithTuple(sortFieldType, tupleOuter, join_col_in2, tupleOuter, join_col_in2) == 0))
    	{
    		 try {
    			    dupOuterBuf.Put(temp1);
    			    dupInnerBuf.Put(temp2);
    			  }
    			  catch (Exception e){
    			    throw new JoinsException(e,"Buffer Pool error");
    	}
    		 if ((temp1 =am1s.get_next()) == null)
 		    {
 		      get_from_outer = true;
 		      
 		    }
    		 else if ((temp2 = am2s.get_next()) == null)
    		 {
    			 get_from_inner = true;
    			 break;
    	}
    	
    	
    	
    	
    	
    	
    	
    	
    	
    	
		if (dupOuterBuf.flush() != 0) {
			Tuple temp21 = new Tuple(); 
				temp21 = am2s.get_next();
			if (PredEval.Eval(outFilter, temp21, temp2, in1, in2)) {
				Projection.Join(temp1, in1, temp21, in2, joinTuple, proj_list, n_out_flds);
				dupOuterBuf.Put(temp2);
				return joinTuple;
			}
		} else {
			temp1 = am1s.get_next();
			while (temp1 != null) {
				if (PredEval.Eval(outFilter, temp1, temp2, in1, in2)) {
					dupOuterBuf.init(matchSpaceInner, 1, len_in2, innerHeap);
					Projection.Join(temp1, in1, temp2, in2, joinTuple, proj_list, n_out_flds);
					dupOuterBuf.Put(temp2);
				
					
					return joinTuple;
				}
				temp1 = am1s.get_next();
			}
		}
		return null;
		
		
		
		 
			      
			      // Note that depending on whether the sort order
			      // is ascending or descending,
			      // this loop will be modified.
			      
			      
			      while (TupleUtils.CompareTupleWithTuple(sortFldType, tuple2,
								      jc_in2, TempTuple2, jc_in2) == 0)
				{
				  // Insert tuple2 into io_buf2
				  
				  try {
				    io_buf2.Put(tuple2);
				  }
				  catch (Exception e){
				    throw new JoinsException(e,"IoBuf error in sortmerge");
				  }
				  if ((tuple2=p_i2.get_next()) == null)
				    {
				      get_from_in2       = true;
				      break;
				    }
				}
			      
			      // tuple1 and tuple2 contain the next tuples to be processed after this set.
			      // Now perform a join of the tuples in io_buf1 and io_buf2.
			      // This is going to be a simple nested loops join with no frills. I guess,
			      // it can be made more efficient, this can be done by a future 564 student.
			      // Another optimization that can be made is to choose the inner and outer
			      // by checking the number of tuples in each equivalence class.
			      
			      if ((_tuple1=io_buf1.Get(TempTuple1)) == null)                // Should not occur
				System.out.println( "Equiv. class 1 in sort-merge has no tuples");
			    }
			  
			  if ((_tuple2 = io_buf2.Get(TempTuple2)) == null)
			    {
			      if (( _tuple1= io_buf1.Get(TempTuple1)) == null)
				{
				  process_next_block = true;
				  continue;                                // Process next equivalence class
				}
			      else
				
			      {
				  io_buf2.reread();
				  _tuple2= io_buf2.Get( TempTuple2);
				}
			    }
			  if (PredEval.Eval(OutputFilter, TempTuple1, TempTuple2, _in1, _in2) == true)
			    {
			      Projection.Join(TempTuple1, _in1, 
					      TempTuple2, _in2, 
					      Jtuple, perm_mat, nOutFlds);
			      return Jtuple;
			    }
			}
		}
	}
	
		
		



/*--------------------------------------------------------------------------*/
    /** 
     *implement the abstract method close() from super class Iterator
     *to finish cleaning up
     *@exception IOException I/O error from lower layers
     *@exception JoinsException join error from lower layers
     *@exception IndexException index access error 
     */

    public void close() 
        throws JoinsException, 
            IOException, IOException, IndexException
    {
    //Unsure where closeFlag is defined
    
    if(!closeFlag){
    try{
    // Attempting to close both Iterators
    am1s.close();
    am2s.close()
    }catch (Exception e){
     throw new JoinsException(e, "Iterator connection was unsuccessful in closing");   
     }
     
     if( outerHeap != null){
     try{
     	outerHeap.deleteFile();
     }
     catch(Exception e)
     {
     	throw new JoinsException(e, "File deletion was unsuccessful");
     }
     
     outerHeap = null;
     
     if (innerHeap != null){
     try{
     	innerHeap.deleteFile();
	}
    catch (Exception e) {
    throw new JoinsException(e, "File deletion was unsuccessful");
    }
    innerHeap = null
    }
    closeFlag = true;
    }
    
    } // End of close
} // End of CLASS SortMerge
