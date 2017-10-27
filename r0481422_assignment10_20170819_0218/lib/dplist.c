#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include "dplist.h"

/*
 * definition of error codes
 * */
#define DPLIST_NO_ERROR 0
#define DPLIST_MEMORY_ERROR 1 // error due to mem alloc failure
#define DPLIST_INVALID_ERROR 2 //error due to a list operation applied on a NULL list 

#ifdef DEBUG
	#define DEBUG_PRINTF(...) 									         \
		do {											         \
			fprintf(stderr,"\nIn %s - function %s at line %d: ", __FILE__, __func__, __LINE__);	 \
			fprintf(stderr,__VA_ARGS__);								 \
			fflush(stderr);                                                                          \
                } while(0)
#else
	#define DEBUG_PRINTF(...) (void)0
#endif


#define DPLIST_ERR_HANDLER(condition,err_code)\
	do {						            \
            if ((condition)) DEBUG_PRINTF(#condition " failed\n");    \
            assert(!(condition));                                    \
        } while(0)

        
/*
 * The real definition of struct list / struct node
 */

struct dplist_node {
  dplist_node_t * prev, * next;
  void * element;
};

struct dplist {
  dplist_node_t * head;
  void * (*element_copy)(void * src_element);			  
  void (*element_free)(void ** element);
  int (*element_compare)(void * x, void * y);
};
dplist_t * dpl_sorted( dplist_t * list);

dplist_t * dpl_create (// callback functions
			  void * (*element_copy)(void * src_element),
			  void (*element_free)(void ** element),
			  int (*element_compare)(void * x, void * y)
			  )
{
  dplist_t * list;
  list = malloc(sizeof(struct dplist));
  DPLIST_ERR_HANDLER(list==NULL,DPLIST_MEMORY_ERROR);
  list->head = NULL;  
  list->element_copy = element_copy;
  list->element_free = element_free;
  list->element_compare = element_compare; 
  return list;
}

void dpl_free(dplist_t ** list, bool free_element)
{
    DPLIST_ERR_HANDLER((list==NULL||*list==NULL),DPLIST_INVALID_ERROR);
    while ( (*list)->head != NULL )
    {
        dpl_remove_at_index(*list, 0,free_element);  //true free to free the memory 
    }
    free(*list);
    *list = NULL;
}

dplist_t * dpl_insert_at_index(dplist_t * list, void * element, int index, bool insert_copy)
{
   dplist_node_t * ref_at_index, * list_node;
  DPLIST_ERR_HANDLER((list==NULL),DPLIST_INVALID_ERROR);
  list_node = malloc(sizeof(dplist_node_t));
  DPLIST_ERR_HANDLER((list_node==NULL),DPLIST_MEMORY_ERROR);
  assert(element != NULL);
 
  if(insert_copy == true)  
  list_node -> element = (*(list -> element_copy))(element);  //use callback function to copy 
  else if(insert_copy == false) 
  list_node -> element = element;							//copy reference 

  if (list->head == NULL)  
  { // case 1 
    list_node->prev = NULL;
    list_node->next = NULL;
    list->head = list_node;
  } else if (index <= 0)  
  { // case 2 
    list_node->prev = NULL;
    list_node->next = list->head;
    list->head->prev = list_node;
    list->head = list_node;
  } 
  else 
  {
    ref_at_index = dpl_get_reference_at_index(list, index);  
    assert( ref_at_index != NULL);
    if (index < dpl_size(list))
    { // covers case 4
      list_node->prev = ref_at_index->prev;
      list_node->next = ref_at_index;
      ref_at_index->prev->next = list_node;
      ref_at_index->prev = list_node;
    } else
    { // covers case 3 
      assert(ref_at_index->next == NULL);
      list_node->next = NULL;
      list_node->prev = ref_at_index;
      ref_at_index->next = list_node;    
    }
  }
  return list;
}

dplist_t * dpl_remove_at_index( dplist_t * list, int index, bool free_element)
{
     dplist_node_t *ref_at_index, *dummy;
  DPLIST_ERR_HANDLER((list==NULL),DPLIST_INVALID_ERROR);
  if (list->head==NULL) return list;  // covers case 1
  ref_at_index = dpl_get_reference_at_index(list, index);
  if (ref_at_index == list->head)    // if to remove the first element
  { // covers case 2
    dummy = list->head;
    list->head = list->head->next;
    if (list->head)  // check if the second element exists
      list->head->prev = NULL;
    //else: list is empty now
  }
  else 
  { // covers case 4
    assert( ref_at_index != NULL);
    dummy = ref_at_index;
    if (ref_at_index->next)
    {
      ref_at_index->next->prev = ref_at_index->prev;
      ref_at_index->prev->next = ref_at_index->next;
    } 
    else // covers case 3 reomve the last element
    {
      ref_at_index->prev->next = NULL; 
    }
  }
  if(free_element == true) 
  (*(list->element_free))(&(dummy->element));   //free the element conditionally
  free(dummy);  
  return list; 
}

int dpl_size( dplist_t * list )
{
     int size = 0;
  dplist_node_t *dummy;
  DPLIST_ERR_HANDLER((list==NULL),DPLIST_INVALID_ERROR);
  for ( dummy = list->head; dummy != NULL; dummy = dummy->next) 
	{
		size++;
	};
  return size;
}

dplist_node_t * dpl_get_reference_at_index( dplist_t * list, int index )
{
    int count;
  dplist_node_t * dummy;
  DPLIST_ERR_HANDLER((list==NULL),DPLIST_INVALID_ERROR);
  if (list->head == NULL ) return NULL;
  dummy = list->head;
  if(index<=0)
  return dummy;
  count = 0; 
  do {
	 if(dummy->next == NULL)
	 return dummy;
	   dummy = dummy->next;
	  count++;
     if (count >= index) return dummy;
	  }
  while(dummy->next != NULL );
  return dummy; 
}

void * dpl_get_element_at_index( dplist_t * list, int index )
{
    dplist_node_t *dummy;
  DPLIST_ERR_HANDLER((list==NULL),DPLIST_INVALID_ERROR);
  dummy = dpl_get_reference_at_index(list, index);
  if (dummy) return dummy->element;
  else return (void *) NULL;
}

int dpl_get_index_of_element( dplist_t * list, void * element )
{
     int index;
  dplist_node_t * dummy;
  DPLIST_ERR_HANDLER((list==NULL),DPLIST_INVALID_ERROR);
  for ( dummy = list->head, index = 0; dummy != NULL; dummy = dummy->next, index++)
  {
	if((*(list->element_compare))(element,dummy->element) == 0) //if the element matches
	return index;    
  }
  return -1; // element not found
}

// HERE STARTS THE EXTRA SET OF OPERATORS //

// ---- list navigation operators ----//
  
dplist_node_t * dpl_get_first_reference( dplist_t * list )
{
    DPLIST_ERR_HANDLER((list==NULL),DPLIST_INVALID_ERROR);
	  
	if (list->head == NULL ) return NULL;
	return list->head;
}

dplist_node_t * dpl_get_last_reference( dplist_t * list )
{
     DPLIST_ERR_HANDLER((list==NULL),DPLIST_INVALID_ERROR);
	if (list->head == NULL ) return NULL;
	
	int size = dpl_size( list );
	return dpl_get_reference_at_index(list, size-1);
}

dplist_node_t * dpl_get_next_reference( dplist_t * list, dplist_node_t * reference )
{
    dplist_node_t * dummy;
	
	DPLIST_ERR_HANDLER((list==NULL),DPLIST_INVALID_ERROR);
	if(reference == NULL) return NULL;
	if (list->head == NULL ) return NULL;
	if(reference -> next == NULL) return NULL;
	
	for(dummy = list->head; dummy != NULL; dummy = dummy->next)
	{
	  if(dummy == reference)
	  return reference->next; 
	}
	return NULL; 
}

dplist_node_t * dpl_get_previous_reference( dplist_t * list, dplist_node_t * reference )
{
    dplist_node_t * dummy;
    
	DPLIST_ERR_HANDLER((list==NULL),DPLIST_INVALID_ERROR);
	if(reference == NULL) return dpl_get_last_reference( list );
	if (list->head == NULL ) return NULL;
	if(reference -> prev == NULL) return NULL;
	
	for(dummy = list->head; dummy != NULL; dummy = dummy->next)
	{
	  if(dummy == reference)
	  return reference->prev; 
	}
	return NULL;
}

// ---- search & find operators ----//  
  
void * dpl_get_element_at_reference( dplist_t * list, dplist_node_t * reference )
{
    dplist_node_t * dummy;
	
   	DPLIST_ERR_HANDLER((list==NULL),DPLIST_INVALID_ERROR);
	if (list->head == NULL ) return NULL;
	if(reference == NULL) return dpl_get_last_reference(list) -> element;
	
	for(dummy = list->head; dummy != NULL; dummy = dummy->next)
	{
	  if(dummy == reference)
	  return reference -> element; 
	}
	return NULL; 
}

dplist_node_t * dpl_get_reference_of_element( dplist_t * list, void * element )
{
    DPLIST_ERR_HANDLER((list==NULL),DPLIST_INVALID_ERROR);
	if (list->head == NULL ) return NULL;
	if(element == NULL) return NULL;
	
	int index = dpl_get_index_of_element(list, element);
	if(index != -1)
	return dpl_get_reference_at_index(list,index);
	return NULL;
}

int dpl_get_index_of_reference( dplist_t * list, dplist_node_t * reference )
{
     dplist_node_t * dummy;
	int count;
   	DPLIST_ERR_HANDLER((list==NULL),DPLIST_INVALID_ERROR);
	if (list->head == NULL ) return -1;
	if(reference == NULL) return dpl_size(list)-1;
	
	for (dummy = list->head, count=0 ;dummy != NULL; dummy = dummy->next)
	{
		// check if the prev and the next match
		if((dummy->prev == reference->prev) && (dummy -> next == reference ->next))
		return count;
		count++;
	}
	return -1;
}
  
// ---- extra insert & remove operators ----//

dplist_t * dpl_insert_at_reference( dplist_t * list, void * element, dplist_node_t * reference, bool insert_copy )
{
    DPLIST_ERR_HANDLER((list == NULL),DPLIST_INVALID_ERROR);
    
	if(element == NULL) return list;
	if(reference == NULL) return dpl_insert_at_index(list, element, dpl_size(list), insert_copy);
	
	int index = dpl_get_index_of_reference(list, reference);
	
	if(index != -1) return dpl_insert_at_index(list, element, index, insert_copy);
	else return list;
}

dplist_t * dpl_insert_sorted( dplist_t * list, void * element, bool insert_copy )
{
    list=dpl_sorted(list);
	list=dpl_insert_at_index(list,  element,0,  insert_copy);
	return  dpl_sorted(list);
}

dplist_t * dpl_remove_at_reference( dplist_t * list, dplist_node_t * reference, bool free_element )
{
   DPLIST_ERR_HANDLER((list==NULL),DPLIST_INVALID_ERROR);
	if(list->head == NULL) return list;
	if(reference == NULL) return dpl_remove_at_index(list,  dpl_size(list)-1, free_element);
	
	int index = dpl_get_index_of_reference(list, reference);
	
	if(index != -1) return dpl_remove_at_index(list, index, free_element);
	else 
	return list;
}

dplist_t * dpl_remove_element( dplist_t * list, void * element, bool free_element )
{
     DPLIST_ERR_HANDLER((list==NULL),DPLIST_INVALID_ERROR);
	if(list->head == NULL) return list;
	if(element == NULL) return list;
	int index = dpl_get_index_of_element(list, element );
	if(index != -1) dpl_remove_at_index( list, index, free_element);
	 return list;
}
  
// ---- you can add your extra operators here ----//
dplist_t * dpl_sorted( dplist_t * list)//sort the list in ascending order
{//return the pointer to the sorted new list
	int count = dpl_size(list);
	dplist_node_t *pMove;
	pMove=list->head;
	while(count>1)
	{
		while(pMove->next!=NULL){
		if(list->element_compare(pMove->element,pMove->next->element)==1)// if a>b
		{
			void *temp;
			temp=pMove->element;
			pMove->element=pMove->next->element;
			pMove->next->element=temp;
			}
			pMove=pMove->next;
		}
		count--;
	//return to the first node
	pMove=list->head;}
	return list;
	}



