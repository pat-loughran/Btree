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

void BTreeIndex::handleAlreadyPresent(std::string indexName, BufMgr *bufMgrIn, const int _attrByteOffset, const Datatype attrType) {
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
        setAttributes(_attrByteOffset, attrType);
}

void BTreeIndex::handleNew(std::string indexName, BufMgr *bufMgrIn, const int _attrByteOffset, const Datatype attrType) {
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
        metaInfo->attrByteOffset = _attrByteOffset;
        metaInfo->attrType = attrType;
        metaInfo->rootPageNo = (PageId)2; //might need to dynamically set this after root created
        metaInfo->numPages = 2;
        bufMgr->unPinPage(file, metaPageNo, true);

        // set Btree instance fields
        setAttributes(_attrByteOffset, attrType);
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
}

void BTreeIndex::createFirstChild(int keyInt, RecordId rid, NonLeafNodeInt* root)
{
    //create first child page manually
    Page* firstPage;
    PageId firstPageId;
    bufMgr->allocPage(file, firstPageId, firstPage);
    LeafNodeInt* firstNode = reinterpret_cast<LeafNodeInt*>(firstPage);

    //initalize leaf node values
    

    // set first entry of child page and unpin
    firstNode->keyArray[0] = keyInt;
    firstNode->ridArray[0] = rid;
    firstNode->rightSibPageNo = 0; // initalize to invalid page number
    bufMgr->unPinPage(file, firstPageId, true);

    // update root and unpin
    root->keyArray[0] = keyInt + 1;
    root->pageNoArray[0] = firstPageId;
    bufMgr->unPinPage(file, rootPageNum, true); //unpin root

    // update numPages in file and instance
    numPages++;
    Page* metaPage;
    bufMgr->readPage(file, (PageId)1, metaPage);
    IndexMetaInfo* metaInfo = reinterpret_cast<IndexMetaInfo*>(metaPage);
    metaInfo->numPages++;
    bufMgr->unPinPage(file, (PageId)1, true);
}

int findInsertIndex(int keyInt, LeafNodeInt* curNode)
{
    for (int i = 0 ; i < INTARRAYLEAFSIZE; i++) {
        // leaf is full, subsequent code will split and handle
        if (curNode->keyArray[INTARRAYLEAFSIZE -1] != INT_MAX) {
            return INTARRAYLEAFSIZE;
        }

        int curKey = curNode->keyArray[i];
        int nextKey = curNode->keyArray[i+1];

        // found empty slot
        if (curKey == INT_MAX) {
            return i;
        }
        // key goes in first element
        if (i == 0 && keyInt < curKey) {
            return 0;
        }

        // key goes inbetween current and next
        if (keyInt > curKey && keyInt < nextKey) {
            return i+1;
        }
    }
}

void BTreeIndex::insertHelper(int index, int keyInt, RecordId rid, NonLeafNodeInt* root, LeafNodeInt* firstNode)
{
    int rootValue = INT_MAX;
    // key is going to be furthest in page
    if (firstNode->keyArray[index] == INT_MAX) {
        //insert key and rid
        firstNode->keyArray[index] = keyInt;
        firstNode->ridArray[index] = rid;
        rootValue = keyInt;
    }
    else {
        for (int i = INTARRAYLEAFSIZE-1; i >= 0; i--) {
            if (firstNode->keyArray[i] != INT_MAX) {
                firstNode->keyArray[i+1] = firstNode->keyArray[i];
                firstNode->ridArray[i+1] = firstNode->ridArray[i];
                if (rootValue == INT_MAX) {
                    rootValue = firstNode->keyArray[i+1];
                }
            }
        }
        firstNode->keyArray[index] = keyInt;
        firstNode->ridArray[index] = rid;
    }
    //update root key
    root->keyArray[0] = rootValue;
    
    //unpin root and firstPage
    bufMgr->unPinPage(file, (PageId)2, true);
    bufMgr->unPinPage(file, (PageId)3, true);

}

bool BTreeIndex::insertInFirstPage(int keyInt, RecordId rid, NonLeafNodeInt* root)
{
    Page* firstPage;
    bufMgr->readPage(file, (PageId)3, firstPage);
    LeafNodeInt* firstNode = reinterpret_cast<LeafNodeInt*>(firstPage);
    int insertIndex = findInsertIndex(keyInt, firstNode);
    if (insertIndex == INTARRAYLEAFSIZE) {
        return false;
    }
    insertHelper(insertIndex, keyInt, rid, root, firstNode);
    return true;

}

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
void initalizeLeafNode(LeafNodeInt* leafNode) {
    for (int i = 0; i <INTARRAYNONLEAFSIZE; i++) {
        leafNode->keyArray[i] = INT_MAX; // pre fill values
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
        handleAlreadyPresent(indexName, bufMgrIn, attrByteOffset, attrType);
        return;
    }
    else {
        handleNew(indexName, bufMgrIn, attrByteOffset, attrType);

        // create BTree
        FileScan fs = FileScan(relationName, bufMgrIn);
        while (true) {
            RecordId rid;
            try {
                fs.scanNext(rid);
                std::string recordStr = fs.getRecord();
                std::string* recordStrPtr = &recordStr;
                void* key = recordStrPtr + attrByteOffset;
                //int keyy = *((int*)key);
                
                insertEntry(key, rid);
            }
            catch (EndOfFileException e) {
                break;
            }
        }
    

    }
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
    // somehow close file scan
    bufMgr->flushFile(file);
    // somehow delete file object
    
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
    int keyInt = *((int*)key);

    // get root page
    // MUST unpin root when done
    Page* rootPage;
    bufMgr->readPage(file, rootPageNum, rootPage);
    NonLeafNodeInt* root = reinterpret_cast<NonLeafNodeInt*>(rootPage);

    // check if this is the first entry, if so, need to create roots first child manually
    if (root->pageNoArray[0] == 0) {
        createFirstChild(keyInt, rid, root);
        return;
    }

    // check if more than one child page exists
    if (numPages == 3) {
        bool keyInserted = insertInFirstPage(keyInt, rid, root);
        if (keyInserted) {
            return;
        }
    }

    // at this point, we have a regualr B+ tree where the root node is always a nonleaf node
    // If this is the first time we are reaching this part of execution, then the root Pages first child is full
    // So we have                   (root)
    //                              /
    //                        (LeafNode)
    // The next Key/Rid pair to be inserted will cause a split that needs to be handled below among any other
    // regular insert logic
    //
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
