/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include <climits>
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

void BTreeIndex::handleAlreadyPresent(std::string indexName, BufMgr *bufMgrIn, std::string relationName, const int _attrByteOffset, const Datatype attrType) {
        // set bufMgr attribute
        bufMgr = bufMgrIn;

        // read meta page from index
        Page* metaPage;
        bufMgr->readPage(file, (PageId)1, metaPage); //read page
        IndexMetaInfo* metaInfo = reinterpret_cast<IndexMetaInfo*>(metaPage);

        // check if arguments are correct
        if (relationName != metaInfo->relationName || _attrByteOffset != metaInfo->attrByteOffset || attrType != metaInfo->attrType) {
            throw BadIndexInfoException("error");
        }
	// set attributes and unpin
        rootPageNum = metaInfo->rootPageNo;
        numPages = metaInfo->numPages;
        bufMgr->unPinPage(file, (PageId)1, false); //unpin page
}

void BTreeIndex::handleNew(std::string indexName, BufMgr *bufMgrIn, std::string relationName, const int _attrByteOffset, const Datatype attrType) {

        // set bufMgr attribute
        bufMgr = bufMgrIn;

        //create header 
        Page* metaPage;
        PageId metaPageNo; // should be 1
        bufMgr->allocPage(file, metaPageNo, metaPage);
        IndexMetaInfo* metaInfo = reinterpret_cast<IndexMetaInfo*>(metaPage);

        //set header page attributes
        strcpy(metaInfo->relationName, relationName.c_str());
        metaInfo->attrByteOffset = _attrByteOffset;
        metaInfo->attrType = attrType;
        metaInfo->rootPageNo = (PageId)2; //might need to dynamically set this after root created
        metaInfo->numPages = 2;
        bufMgr->unPinPage(file, metaPageNo, true);

        // set Btree instance fields
        headerPageNum = metaPageNo; // should be 1
        rootPageNum = (PageId)2;
        attributeType = attrType;
        attrByteOffset = _attrByteOffset;
        numPages = 2;

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
    initalizeLeafNode(firstNode);

    // set first entry of child page and unpin
    firstNode->keyArray[0] = keyInt;
    firstNode->ridArray[0] = rid;
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

int BTreeIndex::findInsertIndex(int keyInt, LeafNodeInt* curNode)
{
    // leaf is full, subsequent code will split and handle
    if (curNode->keyArray[INTARRAYLEAFSIZE -1] != INT_MAX) {
            return INTARRAYLEAFSIZE;
        }
    for (int i = 0 ; i < INTARRAYLEAFSIZE; i++) {
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
    return -1; //Error if code reached
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
        for (int i = INTARRAYLEAFSIZE-1; i >= index; i--) {
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
    root->keyArray[0] = rootValue+1;
    
    //unpin root and firstPage
    bufMgr->unPinPage(file, (PageId)2, true);
    bufMgr->unPinPage(file, root->pageNoArray[0], true);

}

bool BTreeIndex::insertInFirstPage(int keyInt, RecordId rid, NonLeafNodeInt* root)
{
    Page* firstPage;
    bufMgr->readPage(file, root->pageNoArray[0], firstPage);
    LeafNodeInt* firstNode = reinterpret_cast<LeafNodeInt*>(firstPage);
    int insertIndex = findInsertIndex(keyInt, firstNode);
    if (insertIndex == INTARRAYLEAFSIZE) {
        bufMgr->unPinPage(file, root->pageNoArray[0], false);
        return false;
    }
    insertHelper(insertIndex, keyInt, rid, root, firstNode);
    return true;

}


void BTreeIndex::initalizeNonLeafNode(NonLeafNodeInt* nonLeafNode) {
    for (int i = 0; i <INTARRAYNONLEAFSIZE + 1; i++) {
            if (i < INTARRAYNONLEAFSIZE) {
                nonLeafNode->keyArray[i] = INT_MAX; // pre fill values
            }
            nonLeafNode->pageNoArray[i] = 0; // pre fill values
        }
        nonLeafNode->isLeaf = false;
}
void BTreeIndex::initalizeLeafNode(LeafNodeInt* leafNode) {
    for (int i = 0; i <INTARRAYNONLEAFSIZE; i++) {
        leafNode->keyArray[i] = INT_MAX; // pre fill values
        }
    leafNode->rightSibPageNo = 0;
    leafNode->isLeaf = true;
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

    try {
        //BlobFile indexFile = BlobFile::open(indexName);
        //file = &indexFile;
        file = new BlobFile(indexName, false);
        handleAlreadyPresent(indexName, bufMgrIn, relationName, attrByteOffset, attrType);
        return;
    }
    catch (FileNotFoundException &e) {
        //file did not exist, must create new one below
    }

    //create actual Btree file in disc
    file = new BlobFile(indexName, true);
    

    //set up new file
    handleNew(indexName, bufMgrIn, relationName, attrByteOffset, attrType);

    // create BTree
    FileScan fs = FileScan(relationName, bufMgrIn);
    while (true) {
        RecordId rid;
        try {
            fs.scanNext(rid);
            std::string recordStr = fs.getRecord();  
            insertEntry(&recordStr.c_str()[0] + attrByteOffset, rid);
            }
        catch (EndOfFileException &e) {
            break;
        }
    }
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{


    if(scanExecuting){
        endScan();
    }
    // somehow close file scan
    bufMgr->flushFile(file);
    // somehow delete file object
    delete file;
    
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
    int keyInt = *((int*)key);

    // get root page
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
    //remember to unpin the root page and any other pages you visit when you are done with them
}
// -----------------------------------------------------------------------------
// BTreeIndex::startScan Helper
// -----------------------------------------------------------------------------
    void BTreeIndex::locatePage(PageId currPageNumber)
    {
        Page* currPage;
        bufMgr->readPage(file, currPageNumber, currPage);

         NonLeafNodeInt* nleafNode = ( NonLeafNodeInt*)(currPage);

        if(nleafNode->isLeaf)
        {
            currentPageNum = currPageNumber;
            bufMgr->unPinPage(file, currPageNumber, false);
            return;
        }

        for (size_t i = 0; i < INTARRAYNONLEAFSIZE; i++)
        {
            if (i == INTARRAYNONLEAFSIZE || nleafNode->pageNoArray[i+1] == Page::INVALID_NUMBER || nleafNode->keyArray[i] >= lowValInt)
            {
                if (nleafNode->level == 1)
                {
                    currentPageNum = nleafNode->pageNoArray[i];
                    bufMgr->unPinPage(file, currPageNumber, false);
                    return;
                }
                else
                {
                    locatePage(nleafNode->pageNoArray[i]);
                    bufMgr->unPinPage(file, currPageNumber, false);
                    return;
                }

            }

        }
    }
// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
    lowValInt = *((int*)lowValParm);
    highValInt = *((int*)highValParm);
    if ((lowOpParm == LT || lowOpParm == LTE) || (highOpParm == GT || highOpParm == GTE)) {
        throw BadOpcodesException();
    }
    lowOp = lowOpParm;
    highOp = highOpParm;

    if (lowValInt >highValInt) {
        throw BadScanrangeException();
    }
    if(scanExecuting)
    {
        endScan();
    }

    locatePage(rootPageNum);
    bufMgr->readPage(file, currentPageNum, currentPageData);
     LeafNodeInt* node = ( LeafNodeInt*)(currentPageData);
    for (size_t i = 0; i < INTARRAYLEAFSIZE; i++)
    {


        if (((lowOp == GTE && highOp == LTE) && (node->keyArray[i]<= highValInt && node->keyArray[i] >= lowValInt))
        || ((lowOp == GT && highOp == LTE) && (node->keyArray[i]<= highValInt && node->keyArray[i] > lowValInt))
        || ((lowOp == GT && highOp == LTE) && (node->keyArray[i]<= highValInt && node->keyArray[i] > lowValInt))
        || ((lowOp == GT && highOp == LT) && (node->keyArray[i]< highValInt && node->keyArray[i] > lowValInt))
        )
        {
            bufMgr->unPinPage(file, currentPageNum, false);
            nextEntry = i;
            scanExecuting = true;
            return;
        }
    }
    bufMgr->unPinPage(file, currentPageNum, false);
    throw NoSuchKeyFoundException();

}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid) 
{
    if (scanExecuting)
    {
         LeafNodeInt* node = ( LeafNodeInt*)(currentPageData);


        if (node->ridArray[nextEntry].page_number == Page::INVALID_NUMBER)
        {
            nextEntry = INTARRAYLEAFSIZE;
        }


        if (nextEntry == INTARRAYLEAFSIZE)
        {
            if (node->rightSibPageNo != Page::INVALID_NUMBER)
            {
                bufMgr->readPage(file,node->rightSibPageNo, currentPageData);
                currentPageNum = node->rightSibPageNo;
                bufMgr->unPinPage(file, currentPageNum, false);
                nextEntry = 0;
            }
            else
            {
                throw IndexScanCompletedException();
            }
        }
        if (((highOp == LTE && (node->keyArray[nextEntry] <= highValInt)))
            || (highOp == LT && (node->keyArray[nextEntry] < highValInt)))
        {
            outRid = node->ridArray[nextEntry];
            nextEntry++;
        }
        else
        {
            throw IndexScanCompletedException();
        }
    }
    else
        throw ScanNotInitializedException();

}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan() 
{
    if (!scanExecuting){
        throw ScanNotInitializedException();
    }

    scanExecuting = false;
    currentPageData = NULL;
    currentPageNum = Page::INVALID_NUMBER;
}

}
