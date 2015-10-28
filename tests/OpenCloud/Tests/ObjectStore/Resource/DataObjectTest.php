<?php
/**
 * Copyright 2012-2014 Rackspace US, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace OpenCloud\Tests\ObjectStore\Resource;

use Guzzle\Http\Message\Request;
use Guzzle\Http\Message\Response;
use OpenCloud\Common\Constants\Header;
use OpenCloud\ObjectStore\Constants\UrlType;
use OpenCloud\ObjectStore\Exception\ObjectNotEmptyException;
use OpenCloud\ObjectStore\Resource\DataObject;
use OpenCloud\Tests\MockSubscriber;
use OpenCloud\Tests\ObjectStore\ObjectStoreTestCase;

class DataObjectTest extends ObjectStoreTestCase
{
    public function test_Pseudo_Dirs()
    {
        $this->addMockSubscriber($this->makeResponse('[{"subdir": "foo"}]'));
        $list = $this->container->objectList();

        foreach ($list as $object) {
            $this->assertTrue($object->isDirectory());
            $this->assertEquals('foo', $object->getName());
            $this->assertEquals($object->getContainer(), $this->container);
            break;
        }
    }

    /**
     * @mockFile Object
     */
    public function test_Contents()
    {
        $object = $this->container->dataObject('foobar');
        $this->assertEquals('text/html', $object->getContentType());
        $this->assertEquals(512000, $object->getContentLength());
        $this->assertNotNull($object->getEtag());

        $this->assertInstanceOf('OpenCloud\ObjectStore\Resource\DataObject', $object->update());
        $this->assertInstanceOf('Guzzle\Http\Message\Response', $object->delete());
    }

    /**
     * @expectedException OpenCloud\Common\Exceptions\NoNameError
     */
    public function test_Url_Fails()
    {
        $object = $this->container->dataObject();
        $object->getUrl();
    }

    public function test_Copy()
    {
        $object = $this->container->dataObject('foobar');
        $this->assertInstanceOf(
            'Guzzle\Http\Message\Response',
            $object->copy('/new_container/new_object')
        );
    }

    /**
     * @expectedException \OpenCloud\Common\Exceptions\NoNameError
     */
    public function test_Copy_Fails()
    {
        $this->container->dataObject()->copy(null);
    }

    /**
     * @expectedException \OpenCloud\Common\Exceptions\InvalidArgumentError
     */
    public function test_Temp_Url_Fails_With_Incorrect_Method()
    {
        $this->container->dataObject('foobar')->getTemporaryUrl(1000, 'DELETE');
    }

    public function test_Temp_Url_Inherits_Url_Type()
    {
        $service = $this->getClient()->objectStoreService(null, 'IAD', 'internalURL');
        $object = $service->getContainer('foo')->dataObject('bar');

        $this->addMockSubscriber(new Response(204, ['X-Account-Meta-Temp-URL-Key' => 'secret']));

        $tempUrl = $object->getTemporaryUrl(60, 'GET');

        // Check that internal URLs are used
        $this->assertContains('snet-storage', $tempUrl);
    }

    public function test_temp_urls_can_be_forced_to_use_public_urls()
    {
        $service = $this->getClient()->objectStoreService(null, 'IAD', 'internalURL');
        $object = $service->getContainer('foo')->dataObject('bar');

        $this->addMockSubscriber(new Response(204, ['X-Account-Meta-Temp-URL-Key' => 'secret']));

        $tempUrl = $object->getTemporaryUrl(60, 'GET', true);

        // Check that internal URLs are NOT used
        $this->assertNotContains('snet-storage', $tempUrl);
    }

    public function test_Purge()
    {
        $object = $this->container->dataObject('foobar');
        $this->setupCdnContainerMockResponse();
        $this->assertInstanceOf(
            'Guzzle\Http\Message\Response',
            $object->purge('test@example.com')
        );
    }

    public function test_Public_Urls()
    {
        $object = $this->container->dataObject('foobar');

        $this->setupCdnContainerMockResponse();
        $this->assertNotNull($object->getPublicUrl());
        $this->assertNotNull($object->getPublicUrl(UrlType::SSL));
        $this->assertNotNull($object->getPublicUrl(UrlType::STREAMING));
        $this->assertNotNull($object->getPublicUrl(UrlType::IOS_STREAMING));
    }

    public function test_Symlink_To()
    {
        $targetName = 'new_container/new_object';
        $this->addMockSubscriber(new Response(200, array(Header::X_OBJECT_MANIFEST => $targetName)));
        $object = $this->container->dataObject('foobar');
        $this->assertInstanceOf('Guzzle\Http\Message\Response', $object->createSymlinkTo($targetName));
        $this->assertEquals($targetName, $object->getManifest());
    }

    /**
     * @expectedException OpenCloud\Common\Exceptions\NoNameError
     */
    public function test_Symlink_To_Fails_With_NoName()
    {
        $object = $this->container->dataObject()->createSymlinkTo(null);
    }

    /**
     * @expectedException OpenCloud\ObjectStore\Exception\ObjectNotEmptyException
     */
    public function test_Symlink_To_Fails_With_NotEmpty()
    {
        $this->addMockSubscriber(new Response(200, array(Header::CONTENT_LENGTH => 100)));
        $object = $this->container->dataObject('foobar')->createSymlinkTo('new_container/new_object');
    }

    public function test_Symlink_From()
    {
        $symlinkName = 'new_container/new_object';

        // We have to fill the mock response queue to properly get the correct X-Object-Manifest header
        // Container\dataObject( )
        //  - Container\refresh( )
        $this->addMockSubscriber(new Response(200));
        // DataObject\createSymlinkFrom( )
        //  - Container\createRefreshRequest( )
        $this->addMockSubscriber(new Response(200));
        //  - CDNContainer\createRefreshRequest( )
        $this->addMockSubscriber(new Response(200));
        //  - Container\objectExists( )
        $this->addMockSubscriber(new Response(200));
        //  - Container\getPartialObject( )
        $this->addMockSubscriber(new Response(200));
        //  - Container\uploadObject( )
        $this->addMockSubscriber(new Response(200, array(Header::X_OBJECT_MANIFEST => $symlinkName)));

        $object = $this->container->dataObject('foobar')->createSymlinkFrom($symlinkName);
        $this->assertInstanceOf('OpenCloud\ObjectStore\Resource\DataObject', $object);
    }

    /**
     * @expectedException OpenCloud\Common\Exceptions\NoNameError
     */
    public function test_Symlink_From_Fails_With_NoName()
    {
        $object = $this->container->dataObject()->createSymlinkFrom(null);
    }

    /**
     * @expectedException OpenCloud\ObjectStore\Exception\ObjectNotEmptyException
     */
    public function test_Symlink_From_Fails_With_NotEmpty()
    {
        // We have to fill the mock response queue to properly get the correct Content-Length header
        // Container\dataObject( )
        //  - Container\refresh( )
        $this->addMockSubscriber(new Response(200));
        // DataObject\createSymlinkFrom( )
        //  - Container\createRefreshRequest( )
        $this->addMockSubscriber(new Response(200));
        //  - Container\objectExists( )
        $this->addMockSubscriber(new Response(200));
        //  - Container\getPartialObject( )
        $this->addMockSubscriber(new Response(200, array(Header::CONTENT_LENGTH => 100)));

        $object = $this->container->dataObject('foobar')->createSymlinkFrom('new_container/new_object');
    }

    public function test_Can_Get_Delete_At()
    {
        $expectedDeleteTime = \DateTime::createFromFormat('Y-m-d H:i:s', '2010-10-10 10:10:10');
        $this->addMockSubscriber(new Response(200, array(Header::X_DELETE_AT => $expectedDeleteTime->getTimestamp())));
        $actualDeleteTime = $this->container->dataObject('foobar')->getDeleteAtTime();

        $this->assertEquals($expectedDeleteTime, $actualDeleteTime);
    }

    public function test_Can_Set_Delete_At_Time()
    {
        $expectedDeleteTime = \DateTime::createFromFormat('Y-m-d H:i:s', '2010-10-10 10:10:10');
        $this->addMockSubscriber(new Response(200));
        $object = $this->container->dataObject('foobar');
        $this->addMockSubscriber(new Response(200));

        $object->setDeleteAtTime($expectedDeleteTime)->update();

        $mockSubscriber = $this->currentMockSubscriber;
        $receivedRequests = $mockSubscriber->getReceivedRequests();
        $this->assertCount(1, $receivedRequests);
        /** @var Request $request */
        $request = $receivedRequests[0];
        $this->assertEquals($expectedDeleteTime->getTimestamp(), (string) $request->getHeader(Header::X_DELETE_AT));
    }

    public function test_Can_Set_Delete_At_Using_Delete_After()
    {
        $object = $this->container->dataObject('foobar');
        $expected = new \DateTime();
        $expected->add(\DateInterval::createFromDateString('+100 seconds'));
        $object->setDeleteAfter(100);

        $this->assertEquals($expected, $object->getDeleteAtTime());
    }

    public function test_Can_Set_Delete_At_During_Save_Metadata()
    {
        $expectedDeleteTime = \DateTime::createFromFormat('Y-m-d H:i:s', '2010-10-10 10:10:10');
        $this->addMockSubscriber(new Response(200));
        $object = $this->container->dataObject('foobar');
        $this->addMockSubscriber(new Response(200));

        $object->setDeleteAtTime($expectedDeleteTime)->saveMetadata([]);

        $mockSubscriber = $this->currentMockSubscriber;
        $receivedRequests = $mockSubscriber->getReceivedRequests();
        $this->assertCount(1, $receivedRequests);
        /** @var Request $request */
        $request = $receivedRequests[0];
        $this->assertEquals($expectedDeleteTime->getTimestamp(), (string) $request->getHeader(Header::X_DELETE_AT));
    }

    public function test_Can_Get_Content_Encoding()
    {
        $this->addMockSubscriber(new Response(200, array(Header::CONTENT_ENCODING => 'foo')));
        $actualContentEncoding = $this->container->dataObject('foobar')->getContentEncoding();

        $this->assertEquals('foo', $actualContentEncoding);
    }

    public function test_Can_Set_Content_Encoding()
    {
        $this->addMockSubscriber(new Response(200));
        $object = $this->container->dataObject('foobar');
        $this->addMockSubscriber(new Response(200));

        $object->setContentEncoding('foo')->update();

        $mockSubscriber = $this->currentMockSubscriber;
        $receivedRequests = $mockSubscriber->getReceivedRequests();
        $this->assertCount(1, $receivedRequests);
        /** @var Request $request */
        $request = $receivedRequests[0];
        $this->assertEquals('foo', (string) $request->getHeader(Header::CONTENT_ENCODING));
    }

    public function test_Can_Set_Content_Encoding_During_Save_Metadata()
    {
        $this->addMockSubscriber(new Response(200));
        $object = $this->container->dataObject('foobar');
        $this->addMockSubscriber(new Response(200));

        $object->setContentEncoding('foo')->saveMetadata([]);

        $mockSubscriber = $this->currentMockSubscriber;
        $receivedRequests = $mockSubscriber->getReceivedRequests();
        $this->assertCount(1, $receivedRequests);
        /** @var Request $request */
        $request = $receivedRequests[0];
        $this->assertEquals('foo', (string) $request->getHeader(Header::CONTENT_ENCODING));
    }

    public function test_Can_Get_Content_Type()
    {
        $this->addMockSubscriber(new Response(200, array(Header::CONTENT_TYPE => 'foo')));
        $actualContentType = $this->container->dataObject('foobar')->getContentType();

        $this->assertEquals('foo', $actualContentType);
    }

    public function test_Can_Set_Content_Type()
    {
        $this->addMockSubscriber(new Response(200));
        $object = $this->container->dataObject('foobar');
        $this->addMockSubscriber(new Response(200));

        $object->setContentType('foo')->update();

        $mockSubscriber = $this->currentMockSubscriber;
        $receivedRequests = $mockSubscriber->getReceivedRequests();
        $this->assertCount(1, $receivedRequests);
        /** @var Request $request */
        $request = $receivedRequests[0];
        $this->assertEquals('foo', (string) $request->getHeader(Header::CONTENT_TYPE));
    }

    public function test_Can_Set_Content_Type_During_Save_Metadata()
    {
        $this->addMockSubscriber(new Response(200));
        $object = $this->container->dataObject('foobar');
        $this->addMockSubscriber(new Response(200));

        $object->setContentType('foo')->saveMetadata([]);

        $mockSubscriber = $this->currentMockSubscriber;
        $receivedRequests = $mockSubscriber->getReceivedRequests();
        $this->assertCount(1, $receivedRequests);
        /** @var Request $request */
        $request = $receivedRequests[0];
        $this->assertEquals('foo', (string) $request->getHeader(Header::CONTENT_TYPE));
    }

    public function test_Can_Get_Content_Disposition()
    {
        $this->addMockSubscriber(new Response(200, array(Header::CONTENT_DISPOSITION => 'foo')));
        $actualContentDisposition = $this->container->dataObject('foobar')->getContentDisposition();

        $this->assertEquals('foo', $actualContentDisposition);
    }

    public function test_Can_Set_Content_Disposition()
    {
        $this->addMockSubscriber(new Response(200));
        $object = $this->container->dataObject('foobar');
        $this->addMockSubscriber(new Response(200));

        $object->setContentDisposition('foo')->update();

        $mockSubscriber = $this->currentMockSubscriber;
        $receivedRequests = $mockSubscriber->getReceivedRequests();
        $this->assertCount(1, $receivedRequests);
        /** @var Request $request */
        $request = $receivedRequests[0];
        $this->assertEquals('foo', (string) $request->getHeader(Header::CONTENT_DISPOSITION));
    }

    public function test_Can_Set_Content_Disposition_During_Save_Metadata()
    {
        $this->addMockSubscriber(new Response(200));
        $object = $this->container->dataObject('foobar');
        $this->addMockSubscriber(new Response(200));

        $object->setContentDisposition('foo')->saveMetadata([]);

        $mockSubscriber = $this->currentMockSubscriber;
        $receivedRequests = $mockSubscriber->getReceivedRequests();
        $this->assertCount(1, $receivedRequests);
        /** @var Request $request */
        $request = $receivedRequests[0];
        $this->assertEquals('foo', (string) $request->getHeader(Header::CONTENT_DISPOSITION));
    }

    public function test_Can_Set_System_Metadata_When_Retrieving_Metadata()
    {
        $this->addMockSubscriber(new Response(200));

        /** @var DataObject $object */
        $object = $this->container->dataObject('foobar');
        $expectedDeleteAt = new \DateTime();
        $this->addMockSubscriber(new Response(
                200,
                [
                    Header::CONTENT_DISPOSITION => 'content-disposition',
                    Header::CONTENT_TYPE => 'content-type',
                    Header::X_DELETE_AT => $expectedDeleteAt->getTimestamp(),
                    Header::CONTENT_ENCODING => 'content-encoding',
                ]
            )
        );

        $object->retrieveMetadata();

        $this->assertEquals('content-disposition', $object->getContentDisposition());
        $this->assertEquals('content-type', $object->getContentType());
        $this->assertEquals('content-encoding', $object->getContentEncoding());
        $this->assertEquals($expectedDeleteAt, $object->getDeleteAtTime());
    }

}
