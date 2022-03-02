/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"


//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::setAttributes -- private helper
// -----------------------------------------------------------------------------
 
void BTreeIndex::setAttributes(const int _attrByteOffset, const Datatype attrType)
{
    headerPageNum = (PageId)1; // need to ask if this is always true
    attributeType = attrType;
    attrByteOffset = _attrByteOffset;
    leafOccupancy = INTARRAYLEAFSIZE; // need to see if should be capacity or actual
    nodeOccupancy = INTARRAYNONLEAFSIZE; // need to see if should be capacity or actual 
}

void initalizeNonLeafNode(NonLeafNodeInt* nonLeafNode) {
    for (int i = 0; i <INTARRAYNONLEAFSIZE + 1; i++) {
            if (i < INTARRAYNONLEAFSIZE) {
                nonLeafNode->keyArray[i] = INT_MAX; // pre fill values
            }
            nonLeafNode->pageNoArray[i] = 0; // pre fill values
        }
}

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{
    // create name of this index
    std::ostringstream idxStr;
    idxStr << relationName << '.' << attrByteOffset;
    std::string indexName = idxStr.str();
    
    // return indexName
    outIndexName = indexName;
    
    // check if the index file already exists
    if (BlobFile::exists(indexName)) {
        // set file attribute to actual index
        file = &BlobFile::open(indexName);

        // set bufMgr attribute
        bufMgr = bufMgrIn;

        // read meta page from index to fill rootPageNum
        Page* metaPage;
        bufMgr->readPage(file, (PageId)1, metaPage); //read page
        IndexMetaInfo* metaInfo = reinterpret_cast<IndexMetaInfo*>(metaPage);
        rootPageNum = metaInfo->rootPageNo;
        headerPageNum = (PageId)1; // possibly unnecsarry
        numPages = metaInfo->numPages;
        bufMgr->unPinPage(file, (PageId)1, false); //unpin page

        // set btree instance fields
        setAttributes(attrByteOffset, attrType);
        
        return;
    }
    else {
        //create actual Btree file in disc
        file = &BlobFile::create(indexName);
        
        // set bufMgr attribute
        bufMgr = bufMgrIn;

        //create header 
        Page* metaPage;
        PageId metaPageNo; // should be 1
        bufMgr->allocPage(file, metaPageNo, metaPage);
        IndexMetaInfo* metaInfo = reinterpret_cast<IndexMetaInfo*>(metaPage);
        //metaInfo->relationName = relationName.c_str(); Why does this give me an error
        metaInfo->attrByteOffset = attrByteOffset;
        metaInfo->attrType = attrType;
        metaInfo->rootPageNo = (PageId)2; //might need to dynamically set this after root created
        metaInfo->numPages = 2;
        bufMgr->unPinPage(file, metaPageNo, true);

        // set Btree instance fields
        setAttributes(attrByteOffset, attrType);
        numPages = 2;
        headerPageNum = metaPageNo; // should be 1
        rootPageNum = rootPageNum;  // should be 2

        //create root
        Page* root;
        PageId rootPageNo;
        bufMgr->allocPage(file, rootPageNo, root);
        NonLeafNodeInt* rootNode = reinterpret_cast<NonLeafNodeInt*>(root);
        rootNode->level = 1; // How will this be updated once height reaches 3?
        initalizeNonLeafNode(rootNode);
        bufMgr->unPinPage(file, rootPageNo, true);

        //TODO Insert record IDs

    }
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan() 
{

}

}
